package org.apache.ignite.internal.pagemem.size;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;

import static java.util.Arrays.asList;

public abstract class DataStructureSizeUtils {
    /** */
    public static final String GROUP = "group";

    /** */
    public static final String REGION = "region";

    /** */
    public static final String NODE = "node";

    /** */
    public static final String TOTAL = "total";

    /** */
    public static final String INDEX = "indexes";

    /** */
    public static final String INDEX_TREE = "indexes-tree";

    /** */
    public static final String INDEX_REUSE_LIST = "indexes-reuse-list";

    /** */
    public static final String PARTITION = "partition";

    /** */
    public static final String PK_INDEX = "pk-index";

    /** */
    public static final String REUSE_LIST = "reuse-list";

    /** */
    public static final String DATA = "data";

    /** */
    public static final String PURE_DATA = "pure-data";

    /** */
    public static final String INTERNAL = "internal";

    public static final List<String> METRICS = asList(
        TOTAL,
        INDEX,
        INDEX_TREE,
        INDEX_REUSE_LIST,
        PARTITION,
        PK_INDEX,
        REUSE_LIST,
        DATA,
        PURE_DATA,
        INTERNAL
    );

    private DataStructureSizeUtils(){

    }

    public static DataStructureSize sizeWithTrackingPages(
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
//                throw new UnsupportedOperationException();
            }

            @Override public void add(long val) {
                long prev = size.getAndAdd(val);

                if (prev / trackingPages < ((prev + val) / trackingPages))
                    internalSize.add(val * pageSize);
            }

            @Override public long size() {
                return size.get();
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

    public static DataStructureSize doubleSizeUpdate(
        DataStructureSize first,
        DataStructureSize second
    ) {
        return new DataStructureSize() {

            @Override public void inc() {
                first.inc();

                second.inc();
            }

            @Override public void dec() {
                first.dec();

                second.dec();
            }

            @Override public void add(long val) {
                if (val != 0 && (val > 1 || val < -1)) {
                    first.add(val);

                    second.add(val);
                }

                if (val == -1)
                    dec();

                if (val == 1)
                    inc();
            }

            @Override public long size() {
                return first.size();
            }

            @Override public String name() {
                return first.name();
            }
        };
    }

    public static DataStructureSize sizeAndParentUpdate(String name, DataStructureSize delegate) {
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
