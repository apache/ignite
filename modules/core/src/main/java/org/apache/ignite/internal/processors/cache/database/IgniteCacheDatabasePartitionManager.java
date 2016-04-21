package org.apache.ignite.internal.processors.cache.database;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class IgniteCacheDatabasePartitionManager {

    private final int partCount;

    private final ByteBuffer buf;

    private final AtomicReferenceArray<Partition> parts;

    public IgniteCacheDatabasePartitionManager(int partCount, ByteBuffer buf) {
        this.partCount = partCount;
        this.buf = buf;

        parts = new AtomicReferenceArray<Partition>(partCount);

        for (int i = 0; i < partCount; i++) {
            long cntr = buf.getLong(i * 8);

            Partition partition = new Partition(i);

            partition.lastApplied = cntr;

            parts.set(i, partition);
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

    private void updateCounter(int part, long value) {
        assert Thread.holdsLock(parts.get(part));

        buf.putLong(part * 8, value);
    }

    private class Partition {

        private final int id;

        private NavigableMap<Long, Object> updates = new TreeMap<>();

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

            return updates.lastKey();
        }

        public synchronized void onUpdateReceived(long cntr) {
            if (cntr <= lastApplied)
                return;

            if (cntr > lastApplied + 1) {
                updates.put(cntr, null);

                return;
            }

            int delta = 1;

            Iterator<Long> iterator = updates.keySet().iterator();

            while (iterator.hasNext()) {
                long next = iterator.next();

                if (next != lastApplied + delta + 1)
                    break;

                delta++;

                iterator.remove();
            }

            lastApplied += delta;

            updateCounter(id, lastApplied);
        }

    }

}
