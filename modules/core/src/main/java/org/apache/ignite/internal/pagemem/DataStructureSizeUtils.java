package org.apache.ignite.internal.pagemem;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;

public abstract class DataStructureSizeUtils {

    private DataStructureSizeUtils() {

    }

    public static DataStructureSize delegateWithTrackingPages(
        String name,
        DataStructureSize internalSize,
        int pageSize
    ) {
        return new DataStructureSize() {
            private final long trackingPages = TrackingPageIO.VERSIONS.latest().countOfPageToTrack(pageSize);
            private final AtomicLong size = new AtomicLong();

            @Override public void inc() {
                long val = size.getAndIncrement();

                if (val % trackingPages == 0)
                    internalSize.add(pageSize);
            }

            @Override public void dec() {
                throw new UnsupportedOperationException();
            }

            @Override public void add(long val) {
                long prev = size.getAndAdd(val);

                if (prev / trackingPages < ((prev + val) / trackingPages))
                    internalSize.add(val * pageSize);
            }

            @Override public long size() {
                return size.get() + (internalSize.size() / pageSize);
            }

            @Override public String name() {
                return name;
            }
        };
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

    public static DataStructureSize wrap(DataStructureSize size, DataStructureSize delegate) {
        return new DataStructureSize() {

            @Override public void inc() {
                size.inc();

                delegate.inc();
            }

            @Override public void dec() {
                size.dec();

                delegate.dec();
            }

            @Override public void add(long val) {
                if (val != 0 && (val > 1 || val < -1)) {
                    size.add(val);

                    delegate.add(val);
                }

                if (val == -1)
                    dec();

                if (val == 1)
                    inc();
            }

            @Override public long size() {
                return size.size();
            }

            @Override public String name() {
                return size.name();
            }
        };
    }

    public static DataStructureSize delegateTracker(String name, DataStructureSize delegate) {
        return new DataStructureSize() {
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
    }
}
