package org.apache.ignite.internal.processors.cache.database;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Ilya Lantukh
 */
public class IgniteCacheDatabasePartitionManager {

    private final ConcurrentMap<Integer, Partition> parts = new ConcurrentHashMap<>();

    public long getLastAppliedUpdate(int part) {
        Partition partition = parts.get(part);

        if (partition == null)
            return -1;

        return partition.getLastAppliedUpdate();
    }

    public long getLastReceivedUpdate(int part) {
        Partition partition = parts.get(part);

        if (partition == null)
            return -1;

        return partition.getLastReceivedUpdate();
    }

    public void onUpdateReceived(int part, long cntr) {
        Partition partition = parts.get(part);

        if (partition == null) {
            partition = new Partition();

            Partition oldPartition = parts.putIfAbsent(part, partition);

            if (oldPartition != null)
                partition = oldPartition;
        }

        partition.onUpdateReceived(cntr);
    }

    private class Partition {

        private NavigableMap<Long, Object> updates = new TreeMap<>();

        private long lastApplied = -1;

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

            lastApplied++;

            Iterator<Long> iterator = updates.keySet().iterator();

            while (iterator.hasNext()) {
                long next = iterator.next();

                if (next != lastApplied + 1)
                    break;

                lastApplied++;
                iterator.remove();
            }
        }

    }

}
