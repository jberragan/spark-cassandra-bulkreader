package org.apache.cassandra.spark.data.fourzero.types.spark;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.reader.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;

public class VarInt extends org.apache.cassandra.spark.data.fourzero.types.VarInt implements SparkCqlField.DecimalTraits
{
    public static final VarInt INSTANCE = new VarInt();

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createDecimalType(bigNumberConfig.bigIntegerPrecision(), bigNumberConfig.bigIntegerScale());
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof BigInteger)
        {
            return value;
        }
        else if (value instanceof BigDecimal)
        {
            return ((BigDecimal) value).toBigInteger();
        }
        return ((org.apache.spark.sql.types.Decimal) value).toJavaBigInteger();
    }

    @Override
    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return org.apache.spark.sql.types.Decimal.apply((BigInteger) o);
    }

    @Override
    public Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getDecimal(pos, BigNumberConfig.DEFAULT.bigIntegerPrecision(), BigNumberConfig.DEFAULT.bigIntegerScale());
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getDecimal(pos).toBigInteger();
    }
}
