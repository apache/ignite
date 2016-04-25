package org.apache.ignite.internal.processors.cache.database;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.pagemem.Page;
import org.jsr166.LongAdder8;

public class IgniteCacheDatabasePartitionManager {

    private final int partCount;

    private final Page page;

    private final AtomicReferenceArray<Partition> parts;

    public IgniteCacheDatabasePartitionManager(int partCount, Page page) {
        this.partCount = partCount;
        this.page = page;

        parts = new AtomicReferenceArray<Partition>(partCount);

        ByteBuffer buf = page.getForRead();

        try {
            for (int i = 0; i < partCount; i++) {
                long cntr = buf.getLong(i * 8);

                Partition partition = new Partition(i);

                partition.lastApplied.set(cntr);

                parts.set(i, partition);
            }
        }
        finally {
            page.releaseRead();
        }
    }

    public long getLastAppliedUpdate(int part) {
        Partition partition = parts.get(part);

        if (partition == null)
            return 0;

        return partition.getLastAppliedUpdate();
    }

    public long getLastReceivedUpdate(int part) {
        Partition partition = parts.get(part);

        if (partition == null)
            return 0;

        return partition.getLastReceivedUpdate();
    }

    public void onUpdateReceived(int part, long cntr) {
        Partition partition = parts.get(part);

        if (partition == null) {
            partition = new Partition(part);

            boolean set = parts.compareAndSet(part, null, partition);

            if (!set)
                partition = parts.get(part);
        }

        partition.onUpdateReceived(cntr);
    }

    public void flushCounters() {
        ByteBuffer buf = page.getForWrite();

        try {
            for (int i = 0; i < partCount; i++) {
                Partition part = parts.get(i);

                buf.putLong(i * 8, part.getLastAppliedUpdate());
            }
        }
        finally {
            page.releaseWrite(true);
        }
    }

    private class Partition {

        private final int id;

        private ConcurrentNavigableMap<Long, Boolean> updates = new ConcurrentSkipListMap<>();

        private AtomicLong lastApplied = new AtomicLong(0);

        public Partition(int id) {
            this.id = id;
        }

        public long getLastAppliedUpdate() {
            return lastApplied.get();
        }

        public long getLastReceivedUpdate() {
            if (updates.isEmpty())
                return lastApplied.get();

            return updates.lastKey();
        }

        public void onUpdateReceived(long cntr) {
            boolean changed = updates.putIfAbsent(cntr, true) == null;

            if (!changed)
                return;

            while (true) {
                Map.Entry<Long, Boolean> entry = updates.firstEntry();

                if (entry == null)
                    return;

                long first = entry.getKey();

                long cntr0 = lastApplied.get();

                if (first <= cntr0)
                    updates.remove(first);
                else if (first == cntr0 + 1)
                    if (lastApplied.compareAndSet(cntr0, first))
                        updates.remove(first);
                    else
                        break;
                else
                    break;
            }
        }

    }

}
