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

            @Override public void add(long val) {
                size.addAndGet(val);
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

            @Override public void add(long val) {
                if (val != 0 && (val > 1 || val < -1)) {
                    size.addAndGet(val);

                    if (delegate != null)
                        delegate.add(val);
                }

                if (val == -1)
                    dec();

                if (val == 1)
                    inc();
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
