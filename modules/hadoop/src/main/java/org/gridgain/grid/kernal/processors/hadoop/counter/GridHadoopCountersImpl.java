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
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Default in-memory counters store.
 */
public class GridHadoopCountersImpl implements GridHadoopCounters, Externalizable {
    /** Minimal capacity for counters underlying map. */
    public static final int MINIMAL_CAPACITY = 16;

    /** */
    private Map<GridBiTuple<String, String>, GridHadoopCounter> cntrsMap;

    /**
     * Default constructor. Creates new instance without counters.
     */
    public GridHadoopCountersImpl() {
        this.cntrsMap = new HashMap<>(MINIMAL_CAPACITY);
    }

    /**
     * Creates new instance that contain given counters.
     *
     * @param cntrs Counters to store.
     */
    public GridHadoopCountersImpl(Collection<GridHadoopCounter> cntrs) {
        this.cntrsMap =
            new HashMap<>(Math.max(MINIMAL_CAPACITY, cntrs.size()));

        List<GridHadoopCounter> copy = new ArrayList<>(cntrs.size());

        for (GridHadoopCounter cntr : cntrs)
            copy.add(new GridHadoopCounterImpl(cntr.group(), cntr.name(), cntr.value()));

        addCounters(copy);
    }

    /**
     * Copy constructor.
     *
     * @param cntrs Counters to copy.
     */
    public GridHadoopCountersImpl(GridHadoopCounters cntrs) {
        this(cntrs.all());
    }

    /**
     * Adds counters collection in addition to existing counters.
     *
     * @param cntrs Counters to add.
     */
    private void addCounters(Collection<GridHadoopCounter> cntrs) {
        assert cntrs != null;

        for (GridHadoopCounter counter : cntrs)
            cntrsMap.put(new T2<>(counter.group(), counter.name()), counter);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounter counter(String group, String name, boolean create) {
        final T2<String, String> mapKey = new T2<>(group, name);

        GridHadoopCounter counter = cntrsMap.get(mapKey);

        if (counter == null && create) {
            counter = new GridHadoopCounterImpl(group, name, 0);
            cntrsMap.put(mapKey, counter);
        }

        return counter;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridHadoopCounter> all() {
        return cntrsMap.values();
    }

    /** {@inheritDoc} */
    @Override public void merge(GridHadoopCounters other) {
        for (GridHadoopCounter counter : other.all())
            counter(counter.group(), counter.name(), true).increment(counter.value());
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeCollection(out, cntrsMap.values());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException {
        addCounters(U.<GridHadoopCounter>readCollection(in));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GridHadoopCountersImpl counters = (GridHadoopCountersImpl)o;

        if (!cntrsMap.equals(counters.cntrsMap))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return cntrsMap.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopCountersImpl.class, this, "counters", cntrsMap.values());
    }
}
