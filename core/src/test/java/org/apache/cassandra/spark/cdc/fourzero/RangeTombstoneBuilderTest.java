//package org.apache.cassandra.spark.cdc.fourzero;
//
//import org.junit.Test;
//
//import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AsciiType;
//import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
//
//public class RangeTombstoneBuilderTest
//{
//    @Test
//    public void testAddMarker()
//    {
//        RangeTombstoneBuilder builder = new RangeTombstoneBuilder(tableMetadata());
//        builder.add();
//    }
//
//    private TableMetadata tableMetadata()
//    {
//        return TableMetadata.builder("keyspace", "table")
//                            .addPartitionKeyColumn("pk", AsciiType.instance)
//                            .addClusteringColumn("ck", AsciiType.instance)
//                            .build();
//    }
//}
