package org.apache.cassandra.spark.reader.fourzero;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.antlr.runtime.RecognitionException;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.CqlParser;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Keyspace;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableId;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableParams;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Types;
import org.jetbrains.annotations.Nullable;

public class SchemaUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaUtils.class);

    public static boolean isCdcEnabled(CqlTable cqlTable)
    {
        return isCdcEnabled(cqlTable.keyspace(), cqlTable.table());
    }

    public static boolean isCdcEnabled(String keyspace, String table)
    {
        final KeyspaceMetadata ks = Schema.instance.getKeyspaceMetadata(keyspace);
        if (ks == null)
        {
            return false;
        }
        final TableMetadata tb = ks.getTableOrViewNullable(table);
        return tb != null && tb.params.cdc;
    }

    // maps keyspace -> set of table names
    public static Map<String, Set<String>> cdcEnabledTables()
    {
        return Schema.instance.getKeyspaces()
                              .stream()
                              .collect(Collectors.toMap(Function.identity(), SchemaUtils::cdcEnabledTables));
    }

    public static Set<String> cdcEnabledTables(String keyspace)
    {
        return Objects.requireNonNull(Schema.instance.getKeyspaceMetadata(keyspace))
               .tables.stream()
                      .filter(t -> t.params.cdc)
                      .map(f -> f.name)
                      .collect(Collectors.toSet());
    }

    public static boolean keyspaceExists(final String keyspace)
    {
        return getKeyspace(keyspace).isPresent();
    }

    public static boolean tableExists(final String keyspace, final String table)
    {
        return getTable(keyspace, table).isPresent();
    }

    public static Optional<Keyspace> getKeyspace(String keyspace)
    {
        return Optional.ofNullable(Schema.instance.getKeyspaceInstance(keyspace));
    }

    public static Optional<KeyspaceMetadata> getKeyspaceMetadata(String keyspace)
    {
        return getKeyspace(keyspace).map(Keyspace::getMetadata);
    }

    public static Optional<TableMetadata> getTable(String keyspace, String table)
    {
        return Optional.ofNullable(Schema.instance.getTableMetadata(keyspace, table));
    }

    public static boolean has(CqlTable cqlTable)
    {
        return has(cqlTable.keyspace(), cqlTable.table());
    }

    public static boolean has(String keyspace, String table)
    {
        return keyspaceExists(keyspace) && tableExists(keyspace, table);
    }

    public static void maybeUpdateSchema(Partitioner partitioner,
                                         CqlTable cqlTable,
                                         @Nullable final UUID tableId,
                                         boolean enableCdc)
    {
        final String keyspace = cqlTable.keyspace();
        final String table = cqlTable.table();
        Optional<TableMetadata> currTable = getTable(keyspace, table);
        if (currTable.isEmpty())
        {
            throw notExistThrowable(keyspace, table);
        }

        final Set<String> udts = cqlTable.udts()
                                         .stream()
                                         .map(f -> f.createStmt(keyspace))
                                         .collect(Collectors.toSet());
        final TableMetadata updatedTable = buildTableMetadata(keyspace,
                                                              cqlTable.createStmt(),
                                                              buildTypes(keyspace, udts),
                                                              partitioner,
                                                              tableId != null ? tableId : currTable.get().id.asUUID(),
                                                              enableCdc);
        if (updatedTable.equals(currTable.get()))
        {
            // no changes
            return;
        }

        synchronized (SchemaUtils.class)
        {
            final Optional<KeyspaceMetadata> ks = getKeyspaceMetadata(keyspace);
            currTable = getTable(keyspace, table);
            if (ks.isEmpty() || currTable.isEmpty())
            {
                throw notExistThrowable(keyspace, table);
            }
            if (updatedTable.equals(currTable.get()))
            {
                // no changes
                return;
            }

            LOGGER.info("Schema change detected, updating new table schema keyspace={} table={}", keyspace, cqlTable.table());
            Schema.instance.load(ks.get().withSwapped(ks.get().tables.withSwapped(updatedTable)));
        }
    }

    public static Types buildTypes(String keyspace,
                                   Set<String> udtStmts)
    {
        final List<CreateTypeStatement.Raw> typeStatements = new ArrayList<>(udtStmts.size());
        for (final String udt : udtStmts)
        {
            try
            {
                typeStatements.add((CreateTypeStatement.Raw) CQLFragmentParser.parseAnyUnhandled(CqlParser::query, udt));
            }
            catch (final RecognitionException e)
            {
                LOGGER.error("Failed to parse type expression '{}'", udt);
                throw new IllegalStateException(e);
            }
        }
        final Types.RawBuilder typesBuilder = Types.rawBuilder(keyspace);
        for (CreateTypeStatement.Raw st : typeStatements)
        {
            st.addToRawBuilder(typesBuilder);
        }
        return typesBuilder.build();
    }

    public static TableMetadata buildTableMetadata(String keyspace,
                                                   String createStmt,
                                                   Types types,
                                                   Partitioner partitioner,
                                                   @Nullable final UUID tableId,
                                                   boolean enableCdc)
    {
        final TableMetadata.Builder builder = CQLFragmentParser.parseAny(CqlParser::createTableStatement, createStmt, "CREATE TABLE")
                                                               .keyspace(keyspace)
                                                               .prepare(null)
                                                               .builder(types)
                                                               .partitioner(FourZero.getPartitioner(partitioner));

        if (tableId != null)
        {
            builder.id(TableId.fromUUID(tableId));
        }

        final TableParams params = TableParams.builder()
                                              .cdc(enableCdc)
                                              .build();
        return builder.params(params).build();
    }

    public static void enableCdc(CqlTable cqlTable)
    {
        enableCdc(cqlTable.keyspace(), cqlTable.table());
    }

    public static void enableCdc(String keyspace,
                                 String table)
    {
        updateCdc(keyspace, table, true);
    }

    public static void disableCdc(CqlTable cqlTable)
    {
        disableCdc(cqlTable.keyspace(), cqlTable.table());
    }

    public static void disableCdc(String keyspace,
                                  String table)
    {
        updateCdc(keyspace, table, false);
    }

    public static void updateCdc(String keyspace,
                                 String table,
                                 boolean enableCdc)
    {
        if (!has(keyspace, table))
        {
            throw new IllegalArgumentException("Keyspace/table not initialized: " + keyspace + "/" + table);
        }

        Optional<TableMetadata> tb = getTable(keyspace, table);
        if (tb.isEmpty())
        {
            throw notExistThrowable(keyspace, table);
        }
        if (tb.get().params.cdc == enableCdc)
        {
            // nothing to update
            return;
        }

        synchronized (SchemaUtils.class)
        {
            final Optional<KeyspaceMetadata> ks = getKeyspaceMetadata(keyspace);
            tb = getTable(keyspace, table);
            if (ks.isEmpty() || tb.isEmpty())
            {
                throw notExistThrowable(keyspace, table);
            }
            if (tb.get().params.cdc == enableCdc)
            {
                // nothing to update
                return;
            }

            final TableMetadata updatedTable = tb.get().unbuild()
                                                 .params(tb.get().params.unbuild().cdc(enableCdc).build())
                                                 .build();

            LOGGER.info((updatedTable.params.cdc ? "Enabling" : "Disabling") + " CDC for table keyspace={} table={}", keyspace, table);
            Schema.instance.load(ks.get().withSwapped(ks.get().tables.withSwapped(updatedTable)));
        }
    }

    private static IllegalStateException notExistThrowable(String keyspace, String table)
    {
        return new IllegalStateException("Keyspace/table doesn't exist: " + keyspace + "/" + table);
    }
}
