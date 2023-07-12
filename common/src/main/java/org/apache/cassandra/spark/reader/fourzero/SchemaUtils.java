package org.apache.cassandra.spark.reader.fourzero;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.fourzero.FourZeroTypes;
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
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Types;
import org.jetbrains.annotations.Nullable;

public class SchemaUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaUtils.class);

    public static boolean isCdcEnabled(Schema schema, CqlTable cqlTable)
    {
        return isCdcEnabled(schema, cqlTable.keyspace(), cqlTable.table());
    }

    public static boolean isCdcEnabled(Schema schema, String keyspace, String table)
    {
        final KeyspaceMetadata ks = schema.getKeyspaceMetadata(keyspace);
        if (ks == null)
        {
            return false;
        }
        final TableMetadata tb = ks.getTableOrViewNullable(table);
        return tb != null && tb.params.cdc;
    }

    // maps keyspace -> set of table names
    public static Map<String, Set<String>> cdcEnabledTables(Schema schema)
    {
        return Schema.instance.getKeyspaces()
                              .stream()
                              .collect(Collectors.toMap(Function.identity(),
                                                        keyspace -> cdcEnabledTables(schema, keyspace)));
    }

    public static Set<String> cdcEnabledTables(Schema schema, String keyspace)
    {
        return Objects.requireNonNull(schema.getKeyspaceMetadata(keyspace))
               .tables.stream()
                      .filter(t -> t.params.cdc)
                      .map(f -> f.name)
                      .collect(Collectors.toSet());
    }

    public static boolean keyspaceExists(Schema schema, String keyspace)
    {
        return getKeyspace(schema, keyspace).isPresent();
    }

    public static boolean tableExists(Schema schema, String keyspace, String table)
    {
        return getTable(schema, keyspace, table).isPresent();
    }

    public static Optional<Keyspace> getKeyspace(Schema schema, String keyspace)
    {
        return Optional.ofNullable(schema.getKeyspaceInstance(keyspace));
    }

    public static Optional<KeyspaceMetadata> getKeyspaceMetadata(Schema schema, String keyspace)
    {
        return getKeyspace(schema, keyspace).map(Keyspace::getMetadata);
    }

    public static Optional<TableMetadata> getTable(String keyspace, String table)
    {
        return getTable(Schema.instance, keyspace, table);
    }

    public static Optional<TableMetadata> getTable(Schema schema, String keyspace, String table)
    {
        return Optional.ofNullable(schema.getTableMetadata(keyspace, table));
    }

    public static boolean has(Schema schema, CqlTable cqlTable)
    {
        return has(schema, cqlTable.keyspace(), cqlTable.table());
    }

    public static boolean has(Schema schema, String keyspace, String table)
    {
        return keyspaceExists(schema, keyspace) && tableExists(schema, keyspace, table);
    }

    public static void update(Consumer<Schema> updater)
    {
        synchronized (Schema.instance)
        {
            updater.accept(Schema.instance);
        }
    }

    public static <T> T apply(Function<Schema, T> applier)
    {
        synchronized (Schema.instance)
        {
            return applier.apply(Schema.instance);
        }
    }

    public static void maybeUpdateSchema(Schema schema,
                                         Partitioner partitioner,
                                         CqlTable cqlTable,
                                         @Nullable final UUID tableId,
                                         boolean enableCdc)
    {
        final String keyspace = cqlTable.keyspace();
        final String table = cqlTable.table();
        final Optional<TableMetadata> currTable = getTable(schema, keyspace, table);
        if (!currTable.isPresent())
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

        update(s -> {
            final Optional<KeyspaceMetadata> ks = getKeyspaceMetadata(s, keyspace);
            final Optional<TableMetadata> tableOpt = getTable(s, keyspace, table);
            if (!ks.isPresent() || !tableOpt.isPresent())
            {
                throw notExistThrowable(keyspace, table);
            }
            if (updatedTable.equals(tableOpt.get()))
            {
                // no changes
                return;
            }

            LOGGER.info("Schema change detected, updating new table schema keyspace={} table={}", keyspace, cqlTable.table());
            s.load(ks.get().withSwapped(ks.get().tables.withSwapped(updatedTable)));
        });
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
                                                               .partitioner(FourZeroTypes.getPartitioner(partitioner));

        if (tableId != null)
        {
            builder.id(TableId.fromUUID(tableId));
        }

        TableMetadata tableMetadata = builder.build();
        if (tableMetadata.params.cdc == enableCdc)
        {
            return tableMetadata;
        }
        else
        {
            return tableMetadata.unbuild()
                    .params(tableMetadata.params.unbuild()
                            .cdc(enableCdc)
                            .build())
                    .build();
        }
    }

    public static void enableCdc(Schema schema, CqlTable cqlTable)
    {
        enableCdc(schema, cqlTable.keyspace(), cqlTable.table());
    }

    public static void enableCdc(Schema schema,
                                 String keyspace,
                                 String table)
    {
        updateCdc(schema, keyspace, table, true);
    }

    public static void disableCdc(Schema schema, CqlTable cqlTable)
    {
        disableCdc(schema, cqlTable.keyspace(), cqlTable.table());
    }

    public static void disableCdc(Schema schema,
                                  String keyspace,
                                  String table)
    {
        updateCdc(schema, keyspace, table, false);
    }

    public static void updateCdc(Schema schema,
                                 String keyspace,
                                 String table,
                                 boolean enableCdc)
    {
        if (!has(schema, keyspace, table))
        {
            throw new IllegalArgumentException("Keyspace/table not initialized: " + keyspace + "/" + table);
        }

        final Optional<TableMetadata> tb = getTable(schema, keyspace, table);
        if (!tb.isPresent())
        {
            throw notExistThrowable(keyspace, table);
        }
        if (tb.get().params.cdc == enableCdc)
        {
            // nothing to update
            return;
        }

        update(s -> {
            final Optional<KeyspaceMetadata> ks = getKeyspaceMetadata(s, keyspace);
            Optional<TableMetadata> tableOpt = getTable(s, keyspace, table);
            if (!ks.isPresent() || !tableOpt.isPresent())
            {
                throw notExistThrowable(keyspace, table);
            }
            if (tableOpt.get().params.cdc == enableCdc)
            {
                // nothing to update
                return;
            }

            final TableMetadata updatedTable = tableOpt.get().unbuild()
                                                       .params(tableOpt.get().params.unbuild().cdc(enableCdc).build())
                                                       .build();

            LOGGER.info("{} CDC for table keyspace={} table={}",
                        updatedTable.params.cdc ? "Enabling" : "Disabling", keyspace, table);
            s.load(ks.get().withSwapped(ks.get().tables.withSwapped(updatedTable)));
        });
    }

    private static IllegalStateException notExistThrowable(String keyspace, String table)
    {
        return new IllegalStateException("Keyspace/table doesn't exist: " + keyspace + "/" + table);
    }
}
