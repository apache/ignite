package org.apache.ignite.yardstick.cache.load;

import java.util.Map;

/**
 *
 */
public class IgniteWALModesLoadWalDisabledBenchmark extends IgniteWALModesLoadBenchmark {
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        load(cache(), true);

        return false; // Cause benchmark stop.
    }
}
