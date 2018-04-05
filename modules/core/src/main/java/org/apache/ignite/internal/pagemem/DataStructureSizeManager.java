package org.apache.ignite.internal.pagemem;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;

public class DataStructureSizeManager {
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

    /** */
    private final Map<String, DataStructureHolder> sizes = new LinkedHashMap<>();

    public void onCacheGroupCreated(CacheGroupContext grpCtx){
        String cacheOrGroupName = grpCtx.cacheOrGroupName();

        sizes.put(GROUP + "-" + cacheOrGroupName, new DataStructureHolder(cacheOrGroupName, grpCtx.dataRegion().pageMemory().pageSize()));
    }

    public Map<String, DataStructureSize> structureSizes(String structureName) {
        return sizes.get(structureName).sizes;
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

               /* try {
                    PrintStream ps = System.err;

                    String msg = name + " " + size.get() + "\n";

                    ps.write(msg.getBytes());

                    new Exception().printStackTrace(ps);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }*/
            }

            @Override public void dec() {
                size.decrementAndGet();
            }

            @Override public void add(long val) {
                size.addAndGet(val);

           /*     try {
                    PrintStream ps = System.err;

                    String msg = name + " " + size.get() + " " + val + "\n";

                    ps.write(msg.getBytes());

                    new Exception().printStackTrace(ps);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }*/
            }

            @Override public long size() {
                return size.get();
            }

            @Override public String name() {
                return name;
            }
        };
    }

    public static DataStructureSize merge(DataStructureSize first, DataStructureSize second) {
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

    public static class DataStructureHolder {
        /** */
        private final Map<String, DataStructureSize> sizes = new LinkedHashMap<>();

        public DataStructureHolder(String name, int pageSize) {
            String indexesTree = name + "-" + INDEX_TREE;
            String indexesReuseList = name + "-" + INDEX_REUSE_LIST;

            DataStructureSize indexesTreePages = simpleTracker(indexesTree);
            DataStructureSize indexesReuseListPages = simpleTracker(indexesReuseList);

            sizes.put(indexesTree, indexesTreePages);
            sizes.put(indexesReuseList, indexesReuseListPages);

            String pkIndex = name + "-" + PK_INDEX;
            String reuseList = name + "-" + REUSE_LIST;
            String data = name + "-" + DATA;
            String pureData = name + "-" + PURE_DATA;

            DataStructureSize pkIndexPages = simpleTracker(pkIndex);
            DataStructureSize reuseListPages = simpleTracker(reuseList);
            DataStructureSize dataPages = simpleTracker(data);
            DataStructureSize pureDataSize = simpleTracker(pureData);

            sizes.put(pkIndex, pkIndexPages);
            sizes.put(reuseList, reuseListPages);
            sizes.put(data, dataPages);
            sizes.put(pureData, pureDataSize);

            // Internal size.
            String internal = name + "-" + INTERNAL;

            DataStructureSize internalSize = simpleTracker(internal);

            sizes.put(internal, internalSize);

            // Index size.
            String indexesTotal = name + "-" + INDEX;

            DataStructureSize indexTotalPages = delegateWithTrackingPages(indexesTotal, internalSize, pageSize);

            sizes.put(indexesTotal, indexTotalPages);

            // Partitions size.
            String partitionTotal = name + "-" + PARTITION;

            DataStructureSize partitionTotalPages = simpleTracker(partitionTotal);

            sizes.put(partitionTotal, partitionTotalPages);

            // Total size.
            final String total = name + "-" + TOTAL;

            DataStructureSize totalPages = new DataStructureSizeAdapter() {
                @Override public long size() {
                    return (indexTotalPages.size() + partitionTotalPages.size()) * pageSize + internalSize.size();
                }

                @Override public String name() {
                    return total;
                }
            };

            sizes.put(total, totalPages);
        }
    }
}
