/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.counters.CounterGroupBase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

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