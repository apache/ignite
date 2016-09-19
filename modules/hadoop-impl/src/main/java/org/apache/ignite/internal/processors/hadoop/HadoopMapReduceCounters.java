/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.counters.AbstractCounters;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounter;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopLongCounter;
import org.apache.ignite.internal.processors.hadoop.v2.HadoopV2Counter;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * Hadoop counters adapter.
 */
public class HadoopMapReduceCounters extends Counters {
    /** */
    private final Map<T2<String,String>,HadoopLongCounter> cntrs = new HashMap<>();

    /**
     * Creates new instance based on given counters.
     *
     * @param cntrs Counters to adapt.
     */
    public HadoopMapReduceCounters(org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters cntrs) {
        for (HadoopCounter cntr : cntrs.all())
            if (cntr instanceof HadoopLongCounter)
                this.cntrs.put(new T2<>(cntr.group(), cntr.name()), (HadoopLongCounter) cntr);
    }

    /** {@inheritDoc} */
    @Override public synchronized CounterGroup addGroup(CounterGroup grp) {
        return addGroup(grp.getName(), grp.getDisplayName());
    }

    /** {@inheritDoc} */
    @Override public CounterGroup addGroup(String name, String displayName) {
        return new HadoopMapReduceCounterGroup(this, name);
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

        for (HadoopCounter counter : cntrs.values())
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

                return new HadoopMapReduceCounterGroup(HadoopMapReduceCounters.this, iter.next());
            }

            @Override public void remove() {
                throw new UnsupportedOperationException("not implemented");
            }
        };
    }

    /** {@inheritDoc} */
    @Override public synchronized CounterGroup getGroup(String grpName) {
        return new HadoopMapReduceCounterGroup(this, grpName);
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
        if (!(genericRight instanceof HadoopMapReduceCounters))
            return false;

        return cntrs.equals(((HadoopMapReduceCounters) genericRight).cntrs);
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

        for (HadoopCounter counter : cntrs.values()) {
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

        for (HadoopLongCounter counter : cntrs.values()) {
            if (grpName.equals(counter.group()))
                grpCounters.add(new HadoopV2Counter(counter));
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

        HadoopLongCounter internalCntr = cntrs.get(key);

        if (internalCntr == null & create) {
            internalCntr = new HadoopLongCounter(grpName,cntrName);

            cntrs.put(key, new HadoopLongCounter(grpName,cntrName));
        }

        return internalCntr == null ? null : new HadoopV2Counter(internalCntr);
    }
}