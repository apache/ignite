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
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopLongCounter;

/**
 * Hadoop reporter implementation for v1 API.
 */
public class HadoopV1Reporter implements Reporter {
    /** Context. */
    private final HadoopTaskContext ctx;

    /** Input split */
    private final InputSplit split;

    /**
     * Creates new instance.
     *
     * @param ctx Context.
     */
    public HadoopV1Reporter(HadoopTaskContext ctx, InputSplit split) {
        this.ctx = ctx;
        this.split = split;
    }

    /** {@inheritDoc} */
    @Override public void setStatus(String status) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public Counters.Counter getCounter(Enum<?> name) {
        return getCounter(name.getDeclaringClass().getName(), name.name());
    }

    /** {@inheritDoc} */
    @Override public Counters.Counter getCounter(String grp, String name) {
        return new HadoopV1Counter(ctx.counter(grp, name, HadoopLongCounter.class));
    }

    /** {@inheritDoc} */
    @Override public void incrCounter(Enum<?> key, long amount) {
        getCounter(key).increment(amount);
    }

    /** {@inheritDoc} */
    @Override public void incrCounter(String grp, String cntr, long amount) {
        getCounter(grp, cntr).increment(amount);
    }

    /** {@inheritDoc} */
    @Override public InputSplit getInputSplit() throws UnsupportedOperationException {
        return split;
    }

    /** {@inheritDoc} */
    @Override public float getProgress() {
        return 0.5f; // TODO
    }

    /** {@inheritDoc} */
    @Override public void progress() {
        // TODO
    }
}