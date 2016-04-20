package org.apache.ignite.internal.processors.cache.database;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class IgniteCacheDatabasePartitionManager {

    private static final int ENTRY_SIZE = 18;
    private static final int VALUE_OFFSET = 14;
    private static final int START_OFFSET = 6;
    private static final int PART_OFFSET = 2;

    private static final short MAGIC = (short)0xA631;

    private final int partCount;

    private final AtomicReferenceArray<Partition> parts;

    private final AtomicInteger positionGenerator = new AtomicInteger();

    private final ByteBuffer buf;

    public IgniteCacheDatabasePartitionManager(int partCount, ByteBuffer buf) {
        this.partCount = partCount;
        this.buf = buf;

        parts = new AtomicReferenceArray<>(partCount);

        for (int i = 0; i < partCount; i++) {
            long cntrVal = buf.getLong(i * 8);

            Partition part = new Partition(i);

            part.lastApplied = cntrVal;

            parts.set(i, part);
        }

        int position = 0;
        int offset = calculateOffset(position);

        while (offset + ENTRY_SIZE <= buf.limit()) {
            short magic = buf.getShort();

            if (magic != MAGIC) {
                position++;

                offset = calculateOffset(position);

                continue;
            }

            int p = buf.getInt();
            long start = buf.getLong();
            int val = buf.getInt();

            Partition part = parts.get(p);

            assert part != null;

            for (int i = 0; i < val; i++) {
                part.updates.put(start + i, position);
            }
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

            if (!parts.compareAndSet(part, null, partition))
                partition = parts.get(part);
        }

        partition.onUpdateReceived(cntr);
    }

    private void incrementPartitionCounter(int part) {
        long oldVal = buf.getLong(part * 8);

        buf.putLong(part * 8, oldVal + 1);
    }

    private int getFreePosition() {
        return positionGenerator.getAndIncrement();
    }

    private int calculateOffset(int position) {
        return partCount * 8 + (position * ENTRY_SIZE);
    }

    private void clearEntry(int position) {
        int offset = calculateOffset(position);

        for (int i = 0; i < ENTRY_SIZE; i++)
            buf.put(offset + i, (byte)0);
    }

    private void increaseEntrySize(int position, int value, boolean append) {
        int offset = calculateOffset(position);

        if (append) {
            int current = buf.getInt(offset + VALUE_OFFSET);

            buf.putInt(offset + VALUE_OFFSET, current + value);
        }
        else {
            long currentStart = buf.getLong(offset + START_OFFSET);
            int currentVal = buf.getInt(offset + VALUE_OFFSET);

            buf.putLong(offset + START_OFFSET, currentStart - value);
            buf.putInt(offset + VALUE_OFFSET, currentVal + value);
        }
    }

    private void writeEntry(int position, int partition, long start, int initialValue) {
        int offset = calculateOffset(position);

        buf.putShort(offset, MAGIC);
        buf.putInt(offset + PART_OFFSET, partition);
        buf.putLong(offset + START_OFFSET, start);
        buf.putInt(offset + VALUE_OFFSET, initialValue);
    }

    private class Partition {

        private final int id;

        private NavigableMap<Long, Integer> updates = new TreeMap<>();

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
                storeUpdate(cntr);

                return;
            }

            lastApplied++;
            incrementPartitionCounter(id);

            Iterator<Map.Entry<Long, Integer>> iterator = updates.entrySet().iterator();

            int offset = -1;

            while (iterator.hasNext()) {
                Map.Entry<Long, Integer> next = iterator.next();

                if (next.getKey() != lastApplied + 1)
                    break;

                assert offset == -1 || next.getValue() == offset;

                offset = next.getValue();

                lastApplied++;
                incrementPartitionCounter(id);

                iterator.remove();
            }

            if (offset != -1)
                clearEntry(offset);
        }

        private void storeUpdate(long cntr) {
            Integer prev = updates.get(cntr - 1);
            Integer next = updates.get(cntr + 1);

            int position;
            boolean changeNext = false;

            if (prev != null && next != null) {
                assert !prev.equals(next);

                position = prev;
                changeNext = true;
            }
            else if (prev != null) {
                position = prev;

                increaseEntrySize(position, 1, true);
            }
            else if (next != null) {
                position = next;

                increaseEntrySize(position, 1, false);
            }
            else {
                position = getFreePosition();

                writeEntry(position, id, cntr, 1);
            }

            updates.put(cntr, position);

            long prevVal = cntr;

            if (changeNext) {
                int incVal = 1;
                for (Map.Entry<Long, Integer> entry : updates.subMap(prevVal, false, Long.MAX_VALUE, true).entrySet()) {
                    if (entry.getKey() != prevVal + 1)
                        return;

                    entry.setValue(position);
                    prevVal++;
                    incVal++;
                }
                increaseEntrySize(position, incVal, true);
            }
        }

    }

}
