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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Adapter for {@link QueryMetrics}.
 */
public class GridCacheQueryMetricsAdapter implements QueryMetrics, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Minimum time of execution. */
    private final GridAtomicLong minTime = new GridAtomicLong(Long.MAX_VALUE);

    /** Maximum time of execution. */
    private final GridAtomicLong maxTime = new GridAtomicLong();

    /** Sum of execution time for all completed queries. */
    private final LongAdder sumTime = new LongAdder();

    /** Average time of execution.
     * If doesn't equal zero then this metrics set is copy from remote node and doesn't actually update.
     */
    private double avgTime;

    /** Number of executions. */
    private final LongAdder execs = new LongAdder();

    /** Number of completed executions. */
    private final LongAdder completed = new LongAdder();

    /** Number of fails. */
    private final LongAdder fails = new LongAdder();

    /** {@inheritDoc} */
    @Override public long minimumTime() {
        long min = minTime.get();

        return min == Long.MAX_VALUE ? 0 : min;
    }

    /** {@inheritDoc} */
    @Override public long maximumTime() {
        return maxTime.get();
    }

    /** {@inheritDoc} */
    @Override public double averageTime() {
        if (avgTime > 0)
            return avgTime;
        else {
            double val = completed.sum();

            return val > 0 ? sumTime.sum() / val : 0.0;
        }
    }

    /** {@inheritDoc} */
    @Override public int executions() {
        return execs.intValue();
    }

    /**
     * Gets total number of completed executions of query.
     * This value is actual only for local node.
     *
     * @return Number of completed executions.
     */
    public int completedExecutions() {
        return completed.intValue();
    }

    /** {@inheritDoc} */
    @Override public int fails() {
        return fails.intValue();
    }

    /**
     * Update metrics.
     *
     * @param duration Duration of queue execution.
     * @param fail {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public void update(long duration, boolean fail) {
        if (fail) {
            execs.increment();
            fails.increment();
        }
        else {
            execs.increment();
            completed.increment();

            minTime.setIfLess(duration);
            maxTime.setIfGreater(duration);

            sumTime.add(duration);
        }
    }

    /**
     * Merge with given metrics.
     *
     * @return Copy.
     */
    public GridCacheQueryMetricsAdapter copy() {
        GridCacheQueryMetricsAdapter m = new GridCacheQueryMetricsAdapter();

        // Not synchronized because accuracy isn't critical.
        m.fails.add(fails.sum());
        m.minTime.set(minTime.get());
        m.maxTime.set(maxTime.get());
        m.execs.add(execs.sum());
        m.completed.add(completed.sum());
        m.sumTime.add(sumTime.sum());
        m.avgTime = avgTime;

        return m;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(minTime.get());
        out.writeLong(maxTime.get());
        out.writeDouble(averageTime());
        out.writeInt(execs.intValue());
        out.writeInt(fails.intValue());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        minTime.set(in.readLong());
        maxTime.set(in.readLong());
        avgTime = in.readDouble();
        execs.add(in.readInt());
        fails.add(in.readInt());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryMetricsAdapter.class, this);
    }
}