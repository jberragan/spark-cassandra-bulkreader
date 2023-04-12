package org.apache.cassandra.spark.data;

import java.util.Comparator;

public enum AvailabilityHint
{
    // 0 means high priority
    UP(0), MOVING(1), LEAVING(1), UNKNOWN(2), JOINING(2), DOWN(2);

    private final int priority;

    AvailabilityHint(int priority)
    {
        this.priority = priority;
    }

    public static final Comparator<AvailabilityHint> AVAILABILITY_HINT_COMPARATOR = Comparator.comparingInt((AvailabilityHint o) -> o.priority).reversed();

    public static AvailabilityHint fromState(String status, String state)
    {
        if (status.equalsIgnoreCase(AvailabilityHint.DOWN.name()))
        {
            return AvailabilityHint.DOWN;
        }

        if (status.equalsIgnoreCase(AvailabilityHint.UNKNOWN.name()))
        {
            return AvailabilityHint.UNKNOWN;
        }

        if (state.equalsIgnoreCase("NORMAL"))
        {
            return AvailabilityHint.valueOf(status);
        }
        if (state.equalsIgnoreCase(AvailabilityHint.MOVING.name()))
        {
            return AvailabilityHint.MOVING;
        }
        if (state.equalsIgnoreCase(AvailabilityHint.LEAVING.name()))
        {
            return AvailabilityHint.LEAVING;
        }
        if (state.equalsIgnoreCase("STARTING"))
        {
            return AvailabilityHint.valueOf(status);
        }
        if (state.equalsIgnoreCase(AvailabilityHint.JOINING.name()))
        {
            return AvailabilityHint.JOINING;
        }

        return AvailabilityHint.UNKNOWN;
    }
}
