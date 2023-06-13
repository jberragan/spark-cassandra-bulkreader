package org.apache.cassandra.spark.data.fourzero.types;

import java.time.LocalDate;

import org.junit.Test;

import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.SimpleDateSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DateTypeTests
{
    @Test
    public void testDateConversion()
    {
        final int cassandraDate = SimpleDateSerializer.dateStringToDays("2021-07-16");
        assertTrue(cassandraDate < 0);
        assertEquals("2021-07-16", SimpleDateSerializer.instance.toString(cassandraDate));
        final Object sparkSqlDate = Date.INSTANCE.toSparkSqlType(cassandraDate, false);
        assertTrue(sparkSqlDate instanceof Integer);
        final int numDays = (int) sparkSqlDate;
        assertTrue(numDays > 0);
        final LocalDate end = LocalDate.of(1970, 1, 1)
                                       .plusDays(numDays);
        assertEquals(2021, end.getYear());
        assertEquals(7, end.getMonthValue());
        assertEquals(16, end.getDayOfMonth());
        final Object cqlWriterObj = Date.INSTANCE.convertForCqlWriter(numDays, CassandraVersion.FOURZERO);
        final org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.LocalDate cqlWriterDate = (org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.LocalDate) cqlWriterObj;
        assertEquals(2021, cqlWriterDate.getYear());
        assertEquals(7, cqlWriterDate.getMonth());
        assertEquals(16, cqlWriterDate.getDay());
    }
}
