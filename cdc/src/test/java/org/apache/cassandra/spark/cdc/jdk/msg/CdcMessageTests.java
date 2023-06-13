package org.apache.cassandra.spark.cdc.jdk.msg;

import java.util.Date;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.spark.cdc.AbstractCdcEvent;
import org.apache.cassandra.spark.data.fourzero.types.BigInt;
import org.apache.cassandra.spark.data.fourzero.types.Text;
import org.apache.cassandra.spark.data.fourzero.types.Timestamp;
import org.apache.cassandra.spark.data.fourzero.types.UUID;
import org.apache.cassandra.spark.utils.TimeUtils;

import static org.junit.Assert.assertEquals;

public class CdcMessageTests
{
    @Test
    public void testCdcMessage()
    {
        final long colA = (long) BigInt.INSTANCE.randomValue(1024);
        final java.util.UUID colB = java.util.UUID.randomUUID();
        final String colC = Text.INSTANCE.randomValue(1024).toString();
        final Date colD = (Date) Timestamp.INSTANCE.randomValue(1024);
        final long now = TimeUtils.nowMicros();

        final CdcMessage msg = new CdcMessage(
        "ks", "tb",
        ImmutableList.of(new Column("a", BigInt.INSTANCE, colA)),
        ImmutableList.of(new Column("b", UUID.INSTANCE, colB)),
        ImmutableList.of(),
        ImmutableList.of(new Column("c", Text.INSTANCE, colC), new Column("d", Timestamp.INSTANCE, colD)),
        now,
        AbstractCdcEvent.Kind.INSERT,
        ImmutableList.of(), null, null
        );
        assertEquals(4, msg.allColumns().size());
        assertEquals("{\"operation\": INSERT, \"lastModifiedTimestamp\": " + now + ", \"a\": \"" + colA + "\", \"b\": \"" + colB + "\", \"c\": \"" + colC + "\", \"d\": \"" + colD + "\"}",
                     msg.toString());
    }
}
