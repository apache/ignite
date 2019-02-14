package org.apache.ignite.ml.selection.scoring.metric;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Common interface to present metric values for different ML tasks.
 */
public interface MetricValues {
    /** Returns the pair of metric name and metric value. */
    public default Map<String, Double> toMap() {
        Map<String, Double> metricValues = new HashMap<>();
        for (Field field : getClass().getDeclaredFields())
            try {
                metricValues.put(field.getName(), field.getDouble(this));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        return metricValues;
    }
}
