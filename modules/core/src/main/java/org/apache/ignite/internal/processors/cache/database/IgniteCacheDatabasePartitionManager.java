package org.apache.ignite.internal.processors.cache.database;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.pagemem.Page;

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

                partition.lastApplied = cntr;

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

        private NavigableSet<Long> updates = new TreeSet<>();

        private long lastApplied = 0;

        public Partition(int id) {
            this.id = id;
        }

        public synchronized long getLastAppliedUpdate() {
            return lastApplied;
        }

        public synchronized long getLastReceivedUpdate() {
            if (updates.isEmpty())
                return lastApplied;

            return updates.last();
        }

        public synchronized void onUpdateReceived(long cntr) {
            if (cntr <= lastApplied)
                return;

            if (cntr > lastApplied + 1) {
                updates.add(cntr);

                return;
            }

            int delta = 1;

            Iterator<Long> iterator = updates.iterator();

            while (iterator.hasNext()) {
                long next = iterator.next();

                if (next != lastApplied + delta + 1)
                    break;

                delta++;

                iterator.remove();
            }

            lastApplied += delta;
        }

    }

}
