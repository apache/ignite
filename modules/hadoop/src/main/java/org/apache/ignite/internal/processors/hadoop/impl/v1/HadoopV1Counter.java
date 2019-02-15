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

package org.apache.ignite.internal.processors.hadoop.impl.v1;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopLongCounter;
import org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopV2Counter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.mapreduce.util.CountersStrings.toEscapedCompactString;

/**
 * Hadoop counter implementation for v1 API.
 */
public class HadoopV1Counter extends Counters.Counter {
    /** Delegate. */
    private final HadoopLongCounter cntr;

    /**
     * Creates new instance.
     *
     * @param cntr Delegate counter.
     */
    public HadoopV1Counter(HadoopLongCounter cntr) {
        this.cntr = cntr;
    }

    /** {@inheritDoc} */
    @Override public void setDisplayName(String displayName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return cntr.name();
    }

    /** {@inheritDoc} */
    @Override public String getDisplayName() {
        return getName();
    }

    /** {@inheritDoc} */
    @Override public long getValue() {
        return cntr.value();
    }

    /** {@inheritDoc} */
    @Override public void setValue(long val) {
        cntr.value(val);
    }

    /** {@inheritDoc} */
    @Override public void increment(long incr) {
        cntr.increment(incr);
    }

    /** {@inheritDoc} */
    @Override public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    /** {@inheritDoc} */
    @Override public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    /** {@inheritDoc} */
    @Override public String makeEscapedCompactString() {
        return toEscapedCompactString(new HadoopV2Counter(cntr));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public boolean contentEquals(Counters.Counter cntr) {
        return getUnderlyingCounter().equals(cntr.getUnderlyingCounter());
    }

    /** {@inheritDoc} */
    @Override public long getCounter() {
        return cntr.value();
    }

    /** {@inheritDoc} */
    @Override public Counter getUnderlyingCounter() {
        return this;
    }
}