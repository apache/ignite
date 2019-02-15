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

package org.apache.ignite.loadtests;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Load test statistics.
 */
public class GridLoadTestStatistics {
    /** */
    private long taskCnt;

    /** */
    private long jobCnt;

    /** */
    private float avgTaskTime;

    /** */
    private float avgJobTime;

    /** */
    private float minTaskTime = Float.MAX_VALUE;

    /** */
    private float minJobTime = Float.MAX_VALUE;

    /** */
    private float maxTaskTime;

    /** */
    private float maxJobTime;

    /** */
    private long totalTime;

    /** */
    private final Map<UUID, AtomicInteger> nodeCnts = new LinkedHashMap<UUID, AtomicInteger>() {
        /** {@inheritDoc} */
        @Override protected boolean removeEldestEntry(Map.Entry<UUID, AtomicInteger> eldest) {
            return size() > 20;
        }
    };

    /** */
    private final StringBuilder buf = new StringBuilder();

    /**
     * @param fut Task future.
     * @param jobNum Job count.
     * @param taskTime Task execution time.
     * @return Task count.
     */
    @SuppressWarnings("unchecked")
    public synchronized long onTaskCompleted(@Nullable ComputeTaskFuture<?> fut, int jobNum, long taskTime) {
        taskCnt++;

        jobCnt += jobNum;

        totalTime += taskTime;

        avgTaskTime = Math.round(1000.0f * totalTime / taskCnt) / 1000.0f;
        avgJobTime = Math.round(1000.0f * totalTime / jobCnt) / 1000.0f;

        if (taskTime > maxTaskTime)
            maxTaskTime = taskTime;

        if (taskTime < minTaskTime)
            minTaskTime = taskTime;

        float jobTime = Math.round(1000.0f * taskTime / jobNum) / 1000.0f;

        if (jobTime > maxJobTime)
            maxJobTime = jobTime;

        if (jobTime < minJobTime)
            minJobTime = jobTime;

        if (fut != null) {
            Iterable<UUID> nodeIds = (Iterable<UUID>)fut.getTaskSession().getAttribute("nodes");

            if (nodeIds != null) {
                for (UUID id : nodeIds) {
                    AtomicInteger cnt;

                    synchronized (nodeCnts) {
                        cnt = F.addIfAbsent(nodeCnts, id, F.newAtomicInt());
                    }

                    assert cnt != null;

                    cnt.incrementAndGet();
                }
            }
        }

        return taskCnt;
    }

    /**
     * @return Task count.
     */
    public synchronized long getTaskCount() {
        return taskCnt;
    }

    /**
     * @return Job count.
     */
    public synchronized long getJobCount() {
        return jobCnt;
    }

    /**
     * @return Average task time.
     */
    public synchronized float getAverageTaskTime() {
        return avgTaskTime;
    }

    /**
     * @return Average job time.
     */
    public synchronized float getAverageJobTime() {
        return avgJobTime;
    }

    /**
     * @return Minimum task time.
     */
    public synchronized float getMinTaskTime() {
        return minTaskTime;
    }

    /**
     * @return Minimum job time.
     */
    public synchronized float getMinJobTime() {
        return minJobTime;
    }

    /**
     * @return Maximum task time.
     */
    public synchronized float getMaxTaskTime() {
        return maxTaskTime;
    }

    /**
     *
     *  @return Maximum job time.
     */
    public synchronized float getMaxJobTime() {
        return maxJobTime;
    }

    /**
     * @return Test start time.
     */
    public synchronized long getTotalTime() {
        return totalTime;
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        Map<UUID, AtomicInteger> nodeCnts;

        synchronized (this.nodeCnts) {
            nodeCnts = new HashMap<>(this.nodeCnts);
        }

        buf.setLength(0);

        buf.append(getClass().getSimpleName());
        buf.append(" [taskCnt=").append(taskCnt);
        buf.append(", jobCnt=").append(jobCnt);
        buf.append(", avgTaskTime=").append(avgTaskTime);
        buf.append(", avgJobTime=").append(avgJobTime);
        buf.append(", maxTaskTime=").append(maxTaskTime);
        buf.append(", maxJobTime=").append(maxJobTime);
        buf.append(", minTaskTime=").append(minTaskTime);
        buf.append(", minJobTime=").append(minJobTime);
        buf.append(", totalTime=").append(totalTime);
        buf.append(", nodeCnts=").append(nodeCnts);
        buf.append(']');

        return buf.toString();
    }
}
