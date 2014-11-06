/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.hadoop.counter;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.counters.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;

import java.io.*;
import java.util.*;

/**
 * Hadoop Client API Counters adapter.
 */
public class GridHadoopClientCounters extends Counters {
    /** */
    private final GridHadoopCounters cntrs;

    /**
     * Creates new instance based on given counters.
     *
     * @param cntrs Counters to adapt.
     */
    public GridHadoopClientCounters(GridHadoopCounters cntrs) {
        this.cntrs = cntrs;
    }

    /** {@inheritDoc} */
    @Override public synchronized CounterGroup addGroup(CounterGroup group) {
        return addGroup(group.getName(), group.getDisplayName());
    }

    /** {@inheritDoc} */
    @Override public CounterGroup addGroup(String name, String displayName) {
        return new GridHadoopClientCounterGroup(this, name);
    }

    /** {@inheritDoc} */
    @Override public Counter findCounter(String groupName, String counterName) {
        return findCounter(groupName, counterName, true);
    }

    /** {@inheritDoc} */
    @Override public synchronized Counter findCounter(Enum<?> key) {
        return findCounter(key.getDeclaringClass().getName(), key.name(), true);
    }

    /** {@inheritDoc} */
    @Override public synchronized Counter findCounter(String scheme, FileSystemCounter key) {
        return findCounter(String.format("FileSystem Counter (%s)", scheme), key.name());
    }

    /** {@inheritDoc} */
    @Override public synchronized Iterable<String> getGroupNames() {
        Set<String> result = new HashSet<>();

        for (GridHadoopCounter counter : cntrs.all())
            result.add(counter.group());

        return result;
    }

    /** {@inheritDoc} */
    @Override public Iterator<CounterGroup> iterator() {
        final Iterator<String> iter = getGroupNames().iterator();

        return new Iterator<CounterGroup>() {
            @Override public boolean hasNext() {
                return iter.hasNext();
            }

            @Override public CounterGroup next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                return new GridHadoopClientCounterGroup(GridHadoopClientCounters.this, iter.next());
            }

            @Override public void remove() {
                throw new UnsupportedOperationException("not implemented");
            }
        };
    }

    /** {@inheritDoc} */
    @Override public synchronized CounterGroup getGroup(String groupName) {
        return new GridHadoopClientCounterGroup(this, groupName);
    }

    /** {@inheritDoc} */
    @Override public synchronized int countCounters() {
        return cntrs.all().size();
    }

    /** {@inheritDoc} */
    @Override public synchronized void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    /** {@inheritDoc} */
    @Override public synchronized void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    /** {@inheritDoc} */
    @Override public synchronized void incrAllCounters(AbstractCounters<Counter, CounterGroup> other) {
        for (CounterGroup group : other) {
            for (Counter counter : group) {
                findCounter(group.getName(), counter.getName()).increment(counter.getValue());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object genericRight) {
        if (!(genericRight instanceof GridHadoopClientCounters))
            return false;

        return cntrs.all().equals(((GridHadoopClientCounters)genericRight).cntrs.all());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return cntrs.all().hashCode();
    }

    /** {@inheritDoc} */
    @Override public void setWriteAllCounters(boolean send) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean getWriteAllCounters() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Limits limits() {
        return null;
    }

    /**
     * Returns size of a group.
     *
     * @param groupName Name of the group.
     * @return amount of counters in the given group.
     */
    public int groupSize(String groupName) {
        int result = 0;

        for (GridHadoopCounter counter : cntrs.all()) {
            if (groupName.equals(counter.group()))
                result++;
        }

        return result;
    }

    /**
     * Returns counters iterator for specified group.
     *
     * @param groupName Name of the group to iterate.
     * @return Counters iterator.
     */
    public Iterator<Counter> iterateGroup(String groupName) {
        List<Counter> groupCounters = new ArrayList<>();

        for (GridHadoopCounter counter : cntrs.all()) {
            if (groupName.equals(counter.group()))
                groupCounters.add(new GridHadoopV2Counter(counter));
        }

        return groupCounters.iterator();
    }

    /**
     * Find a counter in the group.
     *
     * @param groupName The name of the counter group.
     * @param counterName The name of the counter.
     * @param create Create the counter if not found if true.
     * @return The counter that was found or added or {@code null} if create is false.
     */
    public Counter findCounter(String groupName, String counterName, boolean create) {
        final GridHadoopCounter internalCntr = cntrs.counter(groupName, counterName, create);

        return internalCntr == null ? null : new GridHadoopV2Counter(internalCntr);
    }
}
