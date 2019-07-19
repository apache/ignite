package org.apache.ignite.internal.processors.cache;

public class AdhocUtil {
    public static void assertion(boolean b) {
        if (!b)
            throw new AssertionError();
    }
}
