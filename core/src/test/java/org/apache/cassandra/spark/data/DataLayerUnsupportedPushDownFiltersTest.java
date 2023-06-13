package org.apache.cassandra.spark.data;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.spark.TestDataLayer;
import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;

import static org.apache.cassandra.spark.SparkTestUtils.getFileType;
import static org.apache.cassandra.spark.SparkTestUtils.runTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class DataLayerUnsupportedPushDownFiltersTest
{

    @Test
    public void testNoFilters()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.basic(bridge);
            final List<Path> dataFiles = getFileType(dir, CassandraFile.FileType.DATA).collect(Collectors.toList());
            final TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildSchema());

            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(new Filter[0]);
            assertNotNull(unsupportedFilters);
            assertEquals(0, unsupportedFilters.length);
        });
    }

    @Test
    public void testSupportedEqualToFilter()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.basic(bridge);
            final List<Path> dataFiles = getFileType(dir, CassandraFile.FileType.DATA).collect(Collectors.toList());
            final TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildSchema());

            Filter[] allFilters = { new EqualTo("a", 5) };
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // EqualTo is supported and 'a' is the partition key
            assertEquals(0, unsupportedFilters.length);
        });
    }

    @Test
    public void testSupportedFilterCaseInsensitive()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.basic(bridge);
            final List<Path> dataFiles = getFileType(dir, CassandraFile.FileType.DATA).collect(Collectors.toList());
            final TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildSchema());

            Filter[] allFilters = { new EqualTo("A", 5) };
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // EqualTo is supported and 'a' is the partition key
            assertEquals(0, unsupportedFilters.length);
        });
    }

    @Test
    public void testSupportedInFilter()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.basic(bridge);
            final List<Path> dataFiles = getFileType(dir, CassandraFile.FileType.DATA).collect(Collectors.toList());
            final TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildSchema());

            Filter[] allFilters = { new In("a", new Object[]{ 5, 6, 7 }) };
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // In is supported and 'a' is the partition key
            assertEquals(0, unsupportedFilters.length);
        });
    }

    @Test
    public void testSupportedEqualFilterWithClusteringKey()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.basic(bridge);
            final List<Path> dataFiles = getFileType(dir, CassandraFile.FileType.DATA).collect(Collectors.toList());
            final TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildSchema());

            Filter[] allFilters = { new EqualTo("a", 5), new EqualTo("b", 8) };
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // EqualTo is supported and 'a' is the partition key. The clustering key 'b' is not pushed down
            assertEquals(1, unsupportedFilters.length);
        });
    }

    @Test
    public void testUnsupportedEqualFilterWithColumn()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.basic(bridge);
            final List<Path> dataFiles = getFileType(dir, CassandraFile.FileType.DATA).collect(Collectors.toList());
            final TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildSchema());

            EqualTo unsupportedNonPartitionKeyColumnFilter = new EqualTo("c", 25);
            Filter[] allFilters = { new EqualTo("a", 5), unsupportedNonPartitionKeyColumnFilter };
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // EqualTo is supported and 'a' is the partition key, 'c' is not supported
            assertEquals(1, unsupportedFilters.length);
            assertSame(unsupportedNonPartitionKeyColumnFilter, unsupportedFilters[0]);
        });
    }

    @Test
    public void testUnsupportedFilters()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.basic(bridge);
            final List<Path> dataFiles = getFileType(dir, CassandraFile.FileType.DATA).collect(Collectors.toList());
            final TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildSchema());

            List<Filter> unsupportedFilterList = Arrays.asList(
            new EqualNullSafe("a", 5),
            new GreaterThan("a", 5),
            new GreaterThanOrEqual("a", 5),
            new LessThan("a", 5),
            new LessThanOrEqual("a", 5),
            new IsNull("a"),
            new IsNotNull("a"),
            new And(new EqualTo("a", 5), new EqualTo("b", 6)),
            new Or(new EqualTo("a", 5), new EqualTo("b", 6)),
            new Not(new In("a", new Object[]{ 5, 6, 7 })),
            new StringStartsWith("a", "abc"),
            new StringEndsWith("a", "abc"),
            new StringContains("a", "abc")
            );

            for (Filter unsupportedFilter : unsupportedFilterList)
            {
                Filter[] allFilters = { unsupportedFilter };
                Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
                assertNotNull(unsupportedFilters);
                // not supported
                assertEquals(1, unsupportedFilters.length);
            }
        });
    }

    @Test
    public void testSchemaWithCompositePartitionKey()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = schemaWithCompositePartitionKey(bridge);
            final List<Path> dataFiles = getFileType(dir, CassandraFile.FileType.DATA).collect(Collectors.toList());
            final TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildSchema());

            // a is part of a composite partition column
            Filter[] allFilters = { new EqualTo("a", 5) };
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // filter push-down is disabled because not all partition columns are in the filter array
            assertEquals(1, unsupportedFilters.length);

            // a and b are part of a composite partition column
            allFilters = new Filter[]{ new EqualTo("a", 5), new EqualTo("b", 10) };
            unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // filter push-down is disabled because not all partition columns are in the filter array
            assertEquals(2, unsupportedFilters.length);

            // a and b are part of a composite partition column, but d is not
            allFilters = new Filter[]{ new EqualTo("a", 5), new EqualTo("b", 10), new EqualTo("d", 20) };
            unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // filter push-down is disabled because not all partition columns are in the filter array
            assertEquals(3, unsupportedFilters.length);

            // a and b are part of a composite partition column
            allFilters = new Filter[]{ new EqualTo("a", 5), new EqualTo("b", 10), new EqualTo("c", 15) };
            unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // filter push-down is enabled because all the partition columns are part of the filter array
            assertEquals(0, unsupportedFilters.length);
        });
    }

    @Test
    public void testDisablePushDownWhenPartitionKeyIsMissing()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.basic(bridge);
            final List<Path> dataFiles = getFileType(dir, CassandraFile.FileType.DATA).collect(Collectors.toList());
            final TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildSchema());

            // b is not the partition column
            Filter[] allFilters = { new EqualTo("b", 25) };
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // filter push-down is disabled because the partition column is missing in the filters
            assertEquals(1, unsupportedFilters.length);
        });
    }

    private TestSchema schemaWithCompositePartitionKey(CassandraBridge bridge)
    {
        return TestSchema.builder()
                         .withPartitionKey("a", bridge.aInt())
                         .withPartitionKey("b", bridge.aInt())
                         .withPartitionKey("c", bridge.aInt())
                         .withClusteringKey("d", bridge.aInt())
                         .withColumn("e", bridge.aInt()).build();
    }
}
