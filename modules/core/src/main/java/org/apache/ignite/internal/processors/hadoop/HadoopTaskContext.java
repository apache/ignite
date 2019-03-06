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

package org.apache.ignite.internal.processors.hadoop;

import java.util.Comparator;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounter;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.processors.hadoop.io.PartiallyOffheapRawComparatorEx;

/**
 * Task context.
 */
public abstract class HadoopTaskContext {
    /** */
    protected final HadoopJobEx job;

    /** */
    private HadoopTaskInput input;

    /** */
    private HadoopTaskOutput output;

    /** */
    private HadoopTaskInfo taskInfo;

    /**
     * @param taskInfo Task info.
     * @param job Job.
     */
    protected HadoopTaskContext(HadoopTaskInfo taskInfo, HadoopJobEx job) {
        this.taskInfo = taskInfo;
        this.job = job;
    }

    /**
     * Gets task info.
     *
     * @return Task info.
     */
    public HadoopTaskInfo taskInfo() {
        return taskInfo;
    }

    /**
     * Set a new task info.
     *
     * @param info Task info.
     */
    public void taskInfo(HadoopTaskInfo info) {
        taskInfo = info;
    }

    /**
     * Gets task output.
     *
     * @return Task output.
     */
    public HadoopTaskOutput output() {
        return output;
    }

    /**
     * Gets task input.
     *
     * @return Task input.
     */
    public HadoopTaskInput input() {
        return input;
    }

    /**
     * @return Job.
     */
    public HadoopJobEx job() {
        return job;
    }

    /**
     * Gets counter for the given name.
     *
     * @param grp Counter group's name.
     * @param name Counter name.
     * @return Counter.
     */
    public abstract <T extends HadoopCounter> T counter(String grp, String name, Class<T> cls);

    /**
     * Gets all known counters.
     *
     * @return Unmodifiable collection of counters.
     */
    public abstract HadoopCounters counters();

    /**
     * Sets input of the task.
     *
     * @param in Input.
     */
    public void input(HadoopTaskInput in) {
        input = in;
    }

    /**
     * Sets output of the task.
     *
     * @param out Output.
     */
    public void output(HadoopTaskOutput out) {
        output = out;
    }

    /**
     * Gets partitioner.
     *
     * @return Partitioner.
     * @throws IgniteCheckedException If failed.
     */
    public abstract HadoopPartitioner partitioner() throws IgniteCheckedException;

    /**
     * Gets serializer for values.
     *
     * @return Serializer for keys.
     * @throws IgniteCheckedException If failed.
     */
    public abstract HadoopSerialization keySerialization() throws IgniteCheckedException;

    /**
     * Gets serializer for values.
     *
     * @return Serializer for values.
     * @throws IgniteCheckedException If failed.
     */
    public abstract HadoopSerialization valueSerialization() throws IgniteCheckedException;

    /**
     * Gets sorting comparator.
     *
     * @return Comparator for sorting.
     */
    public abstract Comparator<Object> sortComparator();

    /**
     * Get semi-raw sorting comparator.
     *
     * @return Semi-raw sorting comparator.
     */
    public abstract PartiallyOffheapRawComparatorEx<Object> partialRawSortComparator();

    /**
     * Gets comparator for grouping on combine or reduce operation.
     *
     * @return Comparator.
     */
    public abstract Comparator<Object> groupComparator();

    /**
     * Execute current task.
     *
     * @throws IgniteCheckedException If failed.
     */
    public abstract void run() throws IgniteCheckedException;

    /**
     * Cancel current task execution.
     */
    public abstract void cancel();

    /**
     * Prepare local environment for the task.
     *
     * @throws IgniteCheckedException If failed.
     */
    public abstract void prepareTaskEnvironment() throws IgniteCheckedException;

    /**
     *  Cleans up local environment of the task.
     *
     * @throws IgniteCheckedException If failed.
     */
    public abstract void cleanupTaskEnvironment() throws IgniteCheckedException;

    /**
     * Executes a callable on behalf of the job owner.
     * In case of embedded task execution the implementation of this method
     * will use classes loaded by the ClassLoader this HadoopTaskContext loaded with.
     * @param c The callable.
     * @param <T> The return type of the Callable.
     * @return The result of the callable.
     * @throws IgniteCheckedException On any error in callable.
     */
    public abstract <T> T runAsJobOwner(Callable<T> c) throws IgniteCheckedException;

    /**
     * Callback invoked from mapper thread when map is finished.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void onMapperFinished() throws IgniteCheckedException {
        if (output instanceof HadoopMapperAwareTaskOutput)
            ((HadoopMapperAwareTaskOutput)output).onMapperFinished();
    }
}