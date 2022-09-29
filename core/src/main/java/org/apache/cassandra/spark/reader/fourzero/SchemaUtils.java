package org.apache.cassandra.spark.reader.fourzero;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Keyspace;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.utils.TimeProvider;

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

    public static boolean keyspaceExists(final String keyspaceName)
    {
        return Schema.instance.getKeyspaceInstance(keyspaceName) != null;
    }

    public static boolean tableExists(final String keyspaceName, final String tableName)
    {
        return Objects.requireNonNull(Schema.instance.getKeyspaceMetadata(keyspaceName)).hasTable(tableName);
    }

    private static Keyspace getKeyspace(String keyspaceName)
    {
        return Schema.instance.getKeyspaceInstance(keyspaceName);
    }

    public static boolean has(CqlTable cqlTable)
    {
        return has(cqlTable.keyspace(), cqlTable.table());
    }

    public static boolean has(String keyspace, String table)
    {
        return keyspaceExists(keyspace) && tableExists(keyspace, table);
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

    public synchronized static void updateCdc(String keyspace,
                                              String table,
                                              boolean enableCdc)
    {
        if (!has(keyspace, table))
        {
            throw new IllegalArgumentException("Keyspace/table not initialized: " + keyspace + "/" + table);
        }

        final KeyspaceMetadata ks = Objects.requireNonNull(Schema.instance.getKeyspaceMetadata(keyspace));
        final TableMetadata tb = Objects.requireNonNull(ks.getTableOrViewNullable(table));
        if (tb.params.cdc == enableCdc)
        {
            // nothing to update
            return;
        }
        final TableMetadata updatedTable = tb.unbuild()
                                             .params(tb.params.unbuild().cdc(enableCdc).build())
                                             .build();
        LOGGER.info((updatedTable.params.cdc ? "Enabling" : "Disabling") + " CDC for table keyspace={} table={}", keyspace, table);
        Schema.instance.load(ks.withSwapped(ks.tables.withSwapped(updatedTable)));
    }
}
