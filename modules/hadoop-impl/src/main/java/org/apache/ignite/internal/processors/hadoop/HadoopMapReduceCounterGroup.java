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
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.counters.CounterGroupBase;

/**
 * Hadoop +counter group adapter.
 */
class HadoopMapReduceCounterGroup implements CounterGroup {
    /** Counters. */
    private final HadoopMapReduceCounters cntrs;

    /** Group name. */
    private final String name;

    /**
     * Creates new instance.
     *
     * @param cntrs Client counters instance.
     * @param name Group name.
     */
    HadoopMapReduceCounterGroup(HadoopMapReduceCounters cntrs, String name) {
        this.cntrs = cntrs;
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String getDisplayName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void setDisplayName(String displayName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void addCounter(Counter counter) {
        addCounter(counter.getName(), counter.getDisplayName(), 0);
    }

    /** {@inheritDoc} */
    @Override public Counter addCounter(String name, String displayName, long value) {
        final Counter counter = cntrs.findCounter(this.name, name);

        counter.setValue(value);

        return counter;
    }

    /** {@inheritDoc} */
    @Override public Counter findCounter(String counterName, String displayName) {
        return cntrs.findCounter(name, counterName);
    }

    /** {@inheritDoc} */
    @Override public Counter findCounter(String counterName, boolean create) {
        return cntrs.findCounter(name, counterName, create);
    }

    /** {@inheritDoc} */
    @Override public Counter findCounter(String counterName) {
        return cntrs.findCounter(name, counterName);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return cntrs.groupSize(name);
    }

    /** {@inheritDoc} */
    @Override public void incrAllCounters(CounterGroupBase<Counter> rightGroup) {
        for (final Counter counter : rightGroup)
            cntrs.findCounter(name, counter.getName()).increment(counter.getValue());
    }

    /** {@inheritDoc} */
    @Override public CounterGroupBase<Counter> getUnderlyingGroup() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Counter> iterator() {
        return cntrs.iterateGroup(name);
    }

    /** {@inheritDoc} */
    @Override public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    /** {@inheritDoc} */
    @Override public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }
}