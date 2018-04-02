package org.apache.ignite.internal.pagemem;

import java.util.concurrent.atomic.AtomicLong;

public abstract class DataStructureSizeUtils {

    private DataStructureSizeUtils() {

    }

    public static DataStructureSize simpleTracker(String name) {
        return new DataStructureSize() {
            private final AtomicLong size = new AtomicLong();

            @Override public void inc() {
                size.incrementAndGet();
            }

            @Override public void dec() {
                size.decrementAndGet();
            }

            @Override public long size() {
                return size.get();
            }

            @Override public String name() {
                return name;
            }
        };
    }

    public static DataStructureSize delegateTracker(String name, DataStructureSize delegate) {
        DataStructureSize tr = new DataStructureSize() {
            private final AtomicLong size = new AtomicLong();

            @Override public void inc() {
                size.incrementAndGet();

                if (delegate != null)
                    delegate.inc();
            }

            @Override public void dec() {
                size.decrementAndGet();

                if (delegate != null)
                    delegate.dec();
            }

            @Override public long size() {
                return size.get();
            }

            @Override public String name() {
                return name;
            }
        };

        return tr;
    }
}
