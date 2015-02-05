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

import org.apache.ignite.*;

import java.util.*;

/**
 * Task context.
 */
public abstract class GridHadoopTaskContext {
    /** */
    private final GridHadoopJob job;

    /** */
    private GridHadoopTaskInput input;

    /** */
    private GridHadoopTaskOutput output;

    /** */
    private GridHadoopTaskInfo taskInfo;

    /**
     * @param taskInfo Task info.
     * @param job Job.
     */
    protected GridHadoopTaskContext(GridHadoopTaskInfo taskInfo, GridHadoopJob job) {
        this.taskInfo = taskInfo;
        this.job = job;
    }

    /**
     * Gets task info.
     *
     * @return Task info.
     */
    public GridHadoopTaskInfo taskInfo() {
        return taskInfo;
    }

    /**
     * Set a new task info.
     *
     * @param info Task info.
     */
    public void taskInfo(GridHadoopTaskInfo info) {
        taskInfo = info;
    }

    /**
     * Gets task output.
     *
     * @return Task output.
     */
    public GridHadoopTaskOutput output() {
        return output;
    }

    /**
     * Gets task input.
     *
     * @return Task input.
     */
    public GridHadoopTaskInput input() {
        return input;
    }

    /**
     * @return Job.
     */
    public GridHadoopJob job() {
        return job;
    }

    /**
     * Gets counter for the given name.
     *
     * @param grp Counter group's name.
     * @param name Counter name.
     * @return Counter.
     */
    public abstract <T extends GridHadoopCounter> T counter(String grp, String name, Class<T> cls);

    /**
     * Gets all known counters.
     *
     * @return Unmodifiable collection of counters.
     */
    public abstract GridHadoopCounters counters();

    /**
     * Sets input of the task.
     *
     * @param in Input.
     */
    public void input(GridHadoopTaskInput in) {
        input = in;
    }

    /**
     * Sets output of the task.
     *
     * @param out Output.
     */
    public void output(GridHadoopTaskOutput out) {
        output = out;
    }

    /**
     * Gets partitioner.
     *
     * @return Partitioner.
     * @throws IgniteCheckedException If failed.
     */
    public abstract GridHadoopPartitioner partitioner() throws IgniteCheckedException;

    /**
     * Gets serializer for values.
     *
     * @return Serializer for keys.
     * @throws IgniteCheckedException If failed.
     */
    public abstract GridHadoopSerialization keySerialization() throws IgniteCheckedException;

    /**
     * Gets serializer for values.
     *
     * @return Serializer for values.
     * @throws IgniteCheckedException If failed.
     */
    public abstract GridHadoopSerialization valueSerialization() throws IgniteCheckedException;

    /**
     * Gets sorting comparator.
     *
     * @return Comparator for sorting.
     */
    public abstract Comparator<Object> sortComparator();

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
}
