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
import org.jdk8.backport.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Default in-memory counters store.
 */
public class GridHadoopCountersImpl implements GridHadoopCounters {

    /** */
    private final ConcurrentMap<GridBiTuple<String, String>, GridHadoopCounter> countersMap;

    /**
     * Default constructor. Creates new instance without counters.
     */
    public GridHadoopCountersImpl() {
        countersMap = new ConcurrentHashMap8<>();
    }

    /**
     * Creates new instance that contain given counters.
     * @param counters Counters to store.
     */
    public GridHadoopCountersImpl(Collection<GridHadoopCounter> counters) {
        countersMap = new ConcurrentHashMap8<>(counters.size());

        for (GridHadoopCounter counter : counters) {
            countersMap.put(new T2<>(counter.group(), counter.name()), counter);
        }
    }

    /**
     * Copy constructor.
     *
     * @param counters Source counters to copy from.
     */
    public GridHadoopCountersImpl(GridHadoopCounters counters) {
        this(counters.all());
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounter counter(String group, String name) {
        GridHadoopCounter counter = new GridHadoopCounterImpl(group, name);

        GridHadoopCounter oldCounter =
            countersMap.putIfAbsent(new T2<>(group, name), counter);

        if (oldCounter != null)
            counter = oldCounter;

        return counter;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridHadoopCounter> all() {
        return Collections.unmodifiableCollection(countersMap.values());
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounters merge(GridHadoopCounters other) {
        GridHadoopCountersImpl result = new GridHadoopCountersImpl(this);

        for (GridHadoopCounter counter : other.all()) {
            result.counter(counter.group(), counter.name()).increment(counter.value());
        }

        return result;
    }
}
