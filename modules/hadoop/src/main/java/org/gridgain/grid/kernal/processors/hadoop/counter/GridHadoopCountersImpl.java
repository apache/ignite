/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.counter;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * Default in-memory counters store.
 */
public class GridHadoopCountersImpl implements GridHadoopCounters, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Map<CounterKey, GridHadoopCounter> cntrsMap = new ConcurrentHashMap8<>();

    /**
     * Default constructor. Creates new instance without counters.
     */
    public GridHadoopCountersImpl() {
        // No-op.
    }

    /**
     * Creates new instance that contain given counters.
     *
     * @param cntrs Counters to store.
     */
    public GridHadoopCountersImpl(Iterable<GridHadoopCounter> cntrs) {
        addCounters(cntrs, true);
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
     * Creates counter instance.
     *
     * @param cls Class of the counter.
     * @param grp Group name.
     * @param name Counter name.
     * @return Counter.
     */
    private <T extends GridHadoopCounter> T createCounter(Class<? extends GridHadoopCounter> cls, String grp,
        String name) {
        try {
            Constructor constructor = cls.getConstructor(String.class, String.class);

            return (T)constructor.newInstance(grp, name);
        }
        catch (Exception e) {
            throw new GridRuntimeException(e);
        }
    }

    /**
     * Adds counters collection in addition to existing counters.
     *
     * @param cntrs Counters to add.
     * @param cp Whether to copy counters or not.
     */
    private void addCounters(Iterable<GridHadoopCounter> cntrs, boolean cp) {
        assert cntrs != null;

        for (GridHadoopCounter cntr : cntrs) {
            if (cp) {
                GridHadoopCounter cntrCp = createCounter(cntr.getClass(), cntr.group(), cntr.name());

                cntrCp.merge(cntr);

                cntr = cntrCp;
            }

            cntrsMap.put(new CounterKey(cntr.getClass(), cntr.group(), cntr.name()), cntr);
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends GridHadoopCounter> T counter(String grp, String name, Class<T> cls) {
        assert cls != null;

        CounterKey mapKey = new CounterKey(cls, grp, name);

        T cntr = (T)cntrsMap.get(mapKey);

        if (cntr == null) {
            synchronized (cntrsMap) {
                cntr = (T) cntrsMap.get(mapKey);

                if (cntr == null) {
                    cntr = createCounter(cls, grp, name);

                    cntrsMap.put(mapKey, cntr);
                }
            }
        }

        return cntr;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridHadoopCounter> all() {
        return cntrsMap.values();
    }

    /** {@inheritDoc} */
    @Override public void merge(GridHadoopCounters other) {
        for (GridHadoopCounter counter : other.all())
            counter(counter.group(), counter.name(), counter.getClass()).merge(counter);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeCollection(out, cntrsMap.values());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        addCounters(U.<GridHadoopCounter>readCollection(in), false);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridHadoopCountersImpl counters = (GridHadoopCountersImpl)o;

        return cntrsMap.equals(counters.cntrsMap);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return cntrsMap.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopCountersImpl.class, this, "counters", cntrsMap.values());
    }

    /**
     * The tuple of counter identifier components for more readable code.
     */
    private static class CounterKey extends GridTuple3<Class<? extends GridHadoopCounter>, String, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Constructor.
         *
         * @param cls Class of the counter.
         * @param grp Group name.
         * @param name Counter name.
         */
        private CounterKey(Class<? extends GridHadoopCounter> cls, String grp, String name) {
            super(cls, grp, name);
        }

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public CounterKey() {
            // No-op.
        }
    }
}
