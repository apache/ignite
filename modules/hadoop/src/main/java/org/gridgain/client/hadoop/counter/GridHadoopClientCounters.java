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
import org.gridgain.grid.kernal.processors.hadoop.counter.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.util.*;

/**
 * Hadoop Client API Counters adapter.
 */
public class GridHadoopClientCounters extends Counters {
    /** */
    private final Map<T2<String,String>,GridHadoopLongCounter> cntrs = new HashMap<>();

    /**
     * Creates new instance based on given counters.
     *
     * @param cntrs Counters to adapt.
     */
    public GridHadoopClientCounters(GridHadoopCounters cntrs) {
        for (GridHadoopCounter cntr : cntrs.all())
            if (cntr instanceof GridHadoopLongCounter)
                this.cntrs.put(new T2<>(cntr.group(), cntr.name()), (GridHadoopLongCounter) cntr);
    }

    /** {@inheritDoc} */
    @Override public synchronized CounterGroup addGroup(CounterGroup grp) {
        return addGroup(grp.getName(), grp.getDisplayName());
    }

    /** {@inheritDoc} */
    @Override public CounterGroup addGroup(String name, String displayName) {
        return new GridHadoopClientCounterGroup(this, name);
    }

    /** {@inheritDoc} */
    @Override public Counter findCounter(String grpName, String cntrName) {
        return findCounter(grpName, cntrName, true);
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
        Collection<String> res = new HashSet<>();

        for (GridHadoopCounter counter : cntrs.values())
            res.add(counter.group());

        return res;
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
    @Override public synchronized CounterGroup getGroup(String grpName) {
        return new GridHadoopClientCounterGroup(this, grpName);
    }

    /** {@inheritDoc} */
    @Override public synchronized int countCounters() {
        return cntrs.size();
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

        return cntrs.equals(((GridHadoopClientCounters) genericRight).cntrs);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return cntrs.hashCode();
    }

    /** {@inheritDoc} */
    @Override public void setWriteAllCounters(boolean snd) {
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
     * @param grpName Name of the group.
     * @return amount of counters in the given group.
     */
    public int groupSize(String grpName) {
        int res = 0;

        for (GridHadoopCounter counter : cntrs.values()) {
            if (grpName.equals(counter.group()))
                res++;
        }

        return res;
    }

    /**
     * Returns counters iterator for specified group.
     *
     * @param grpName Name of the group to iterate.
     * @return Counters iterator.
     */
    public Iterator<Counter> iterateGroup(String grpName) {
        Collection<Counter> grpCounters = new ArrayList<>();

        for (GridHadoopLongCounter counter : cntrs.values()) {
            if (grpName.equals(counter.group()))
                grpCounters.add(new GridHadoopV2Counter(counter));
        }

        return grpCounters.iterator();
    }

    /**
     * Find a counter in the group.
     *
     * @param grpName The name of the counter group.
     * @param cntrName The name of the counter.
     * @param create Create the counter if not found if true.
     * @return The counter that was found or added or {@code null} if create is false.
     */
    public Counter findCounter(String grpName, String cntrName, boolean create) {
        T2<String, String> key = new T2<>(grpName, cntrName);

        GridHadoopLongCounter internalCntr = cntrs.get(key);

        if (internalCntr == null & create) {
            internalCntr = new GridHadoopLongCounter(grpName,cntrName);

            cntrs.put(key, new GridHadoopLongCounter(grpName,cntrName));
        }

        return internalCntr == null ? null : new GridHadoopV2Counter(internalCntr);
    }
}
