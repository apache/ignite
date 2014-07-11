// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.counter;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.util.*;

/**
 * Default in-memory counters store.
 */
public class GridHadoopCountersImpl implements GridHadoopCounters, Externalizable {
    /** */
    private Map<GridBiTuple<String, String>, GridHadoopCounter> countersMap;

    /**
     * Default constructor. Creates new instance without counters.
     */
    public GridHadoopCountersImpl() {
        this(Collections.<GridHadoopCounter>emptyList());
    }

    /**
     * Creates new instance that contain given counters.
     *
     * @param counters Counters to store.
     */
    public GridHadoopCountersImpl(Collection<GridHadoopCounter> counters) {
        countersMap = mapCounters(counters);
    }

    /**
     * Converts collection of counters into map.
     *
     * @param counters Counters.
     * @return Map where key is a pair of counter group and name, value is the counter itself.
     */
    private Map<GridBiTuple<String, String>, GridHadoopCounter> mapCounters(Collection<GridHadoopCounter> counters) {
        assert counters != null;

        Map<GridBiTuple<String, String>, GridHadoopCounter> result = new HashMap<>(counters.size());

        for (GridHadoopCounter counter : counters) {
            result.put(new T2<>(counter.group(), counter.name()), counter);
        }

        return result;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounter counter(String group, String name) {
        final T2<String, String> mapKey = new T2<>(group, name);

        GridHadoopCounter counter = countersMap.get(mapKey);

        if (counter == null) {
            counter = new GridHadoopCounterImpl(group, name);
            countersMap.put(mapKey, counter);
        }

        return counter;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridHadoopCounter> all() {
        return Collections.unmodifiableCollection(countersMap.values());
    }

    /** {@inheritDoc} */
    @Override public void merge(GridHadoopCounters other) {
        for (GridHadoopCounter counter : other.all()) {
            counter(counter.group(), counter.name()).increment(counter.value());
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(new ArrayList<>(countersMap.values()));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException {
        countersMap = mapCounters((Collection<GridHadoopCounter>)in.readObject());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GridHadoopCountersImpl counters = (GridHadoopCountersImpl)o;

        if (!countersMap.equals(counters.countersMap))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return countersMap.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Counters [" + countersMap.values() + ']';
    }
}
