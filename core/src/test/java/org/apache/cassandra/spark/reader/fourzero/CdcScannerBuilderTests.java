package org.apache.cassandra.spark.reader.fourzero;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;

import org.junit.Test;

import org.apache.cassandra.spark.TestDataLayer;
import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.cassandra.spark.cdc.SparkCdcEvent;
import org.apache.cassandra.spark.cdc.watermarker.DoNothingWatermarker;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Clustering;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Mutation;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Cell;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.TimeUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CdcScannerBuilderTests
{
    @Test
    public void testInit()
    {
        final ICassandraSource cassandraSource = (keySpace, table, columnsToFetch, primaryKeyColumns) -> null;
        final SparkCdcScannerBuilder cdcScannerBuilder = new SparkCdcScannerBuilder(0,
                                                                                    Partitioner.Murmur3Partitioner,
                                                                                    Stats.DoNothingStats.INSTANCE,
                                                                                    null,
                                                                                    CdcOffsetFilter.of(Collections.emptyMap(), Collections.emptyMap(), TimeUtils.nowMicros(), Duration.ofSeconds(60)),
                                                                                    (ks) -> 1,
                                                                                    DoNothingWatermarker.INSTANCE,
                                                                                    "101",
                                                                                    TestDataLayer.EXECUTOR,
                                                                                    false,
                                                                                    Collections.emptyMap(),
                                                                                    cassandraSource);
        try (final IStreamScanner<SparkCdcEvent> scanner = cdcScannerBuilder.build())
        {
            while (scanner.next())
            {
                scanner.data();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMakeMutation()
    {
        final FourZero bridge = (FourZero) CassandraBridge.get(CassandraVersion.FOURZERO);
        final TestSchema schema = TestSchema.builder().withPartitionKey("a", bridge.text())
                                            .withClusteringKey("b", bridge.aInt())
                                            .withColumn("c", bridge.aInt())
                                            .withColumn("d", bridge.text())
                                            .withCdc(true)
                                            .build();
        schema.schemaBuilder(Partitioner.Murmur3Partitioner);
        final CqlTable cqlTable = schema.buildSchema();
        final TableMetadata tableMetadata = SchemaUtils.getTable(cqlTable.keyspace(), cqlTable.table()).orElseThrow(RuntimeException::new);
        final TestSchema.TestRow row = schema.randomRow();
        final Mutation mutation = bridge.makeMutation(cqlTable, row, TimeUtils.nowMicros());
        assertNotNull(mutation);
        final PartitionUpdate update = mutation.getPartitionUpdate(tableMetadata);
        assertNotNull(update);
        assertEquals(2, update.columns().regulars.size());
        assertEquals("c", update.columns().regulars.getSimple(0).name.toString());
        assertEquals("d", update.columns().regulars.getSimple(1).name.toString());
        final Row mutationRow = update.getRow(Clustering.make(bridge.aInt().serialize(row.get(1))));
        assertNotNull(mutationRow);
        final Cell<?> cCell = mutationRow.getCell(tableMetadata.getColumn(bridge.text().serialize("c")));
        assertEquals(row.get(2), bridge.aInt().deserialize(cCell.buffer()));
        final Cell<?> dCell = mutationRow.getCell(tableMetadata.getColumn(bridge.text().serialize("d")));
        assertEquals(row.get(3), Objects.requireNonNull(bridge.text().deserialize(dCell.buffer())).toString());
    }
}
