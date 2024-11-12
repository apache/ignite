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

package org.apache.ignite.internal.processors.task;

import java.util.Collection;
import java.util.Optional;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

/** */
public class TaskExecutionOptions {
    /** */
    private String name;

    /** */
    private long timeout;

    /** */
    private String execName;

    /** */
    private Byte pool;

    /** */
    private Collection<ClusterNode> projection;

    /** */
    private IgnitePredicate<ClusterNode> projectionPredicate;

    /** */
    private boolean isFailoverDisabled;

    /** */
    private boolean isResultCacheDisabled;

    /** */
    private boolean isSysTask;

    /** */
    private boolean isPublicReq;

    /** */
    private TaskExecutionOptions() {}

    /** */
    private TaskExecutionOptions(TaskExecutionOptions other) {
        name = other.name;
        timeout = other.timeout;
        execName = other.execName;
        pool = other.pool;
        projection = other.projection;
        projectionPredicate = other.projectionPredicate;
        isFailoverDisabled = other.isFailoverDisabled;
        isResultCacheDisabled = other.isResultCacheDisabled;
        isSysTask = other.isSysTask;
        isPublicReq = other.isPublicReq;
    }

    /** */
    public static TaskExecutionOptions options() {
        return new TaskExecutionOptions();
    }

    /** */
    public static TaskExecutionOptions options(Collection<ClusterNode> projection) {
        return new TaskExecutionOptions().withProjection(projection);
    }

    /** */
    public static TaskExecutionOptions options(TaskExecutionOptions other) {
        return new TaskExecutionOptions(other);
    }

    /** */
    public long timeout() {
        return timeout;
    }

    /** */
    public TaskExecutionOptions withTimeout(long timeout) {
        this.timeout = timeout;

        return this;
    }

    /** */
    public Optional<String> name() {
        return Optional.ofNullable(name);
    }

    /** */
    public TaskExecutionOptions withName(String name) {
        this.name = name;

        return this;
    }

    /** */
    public Collection<ClusterNode> projection() {
        return projection;
    }

    /** */
    public TaskExecutionOptions withProjection(Collection<ClusterNode> projection) {
        this.projection = projection;

        return this;
    }

    /** */
    public IgnitePredicate<ClusterNode> projectionPredicate() {
        return projectionPredicate;
    }

    /** */
    public TaskExecutionOptions withProjectionPredicate(IgnitePredicate<ClusterNode> projectionPredicate) {
        this.projectionPredicate = projectionPredicate;

        return this;
    }

    /** */
    public String executor() {
        return execName;
    }

    /** */
    public TaskExecutionOptions withExecutor(String execName) {
        this.execName = execName;

        return this;
    }

    /** */
    public Optional<Byte> pool() {
        return Optional.ofNullable(pool);
    }

    /** */
    public TaskExecutionOptions withPool(byte pool) {
        this.pool = pool;

        return this;
    }

    /** */
    public boolean isFailoverDisabled() {
        return isFailoverDisabled;
    }

    /** */
    public TaskExecutionOptions withFailoverDisabled() {
        isFailoverDisabled = true;

        return this;
    }

    /** */
    public boolean isResultCacheDisabled() {
        return isResultCacheDisabled;
    }

    /** */
    public TaskExecutionOptions withResultCacheDisabled() {
        isResultCacheDisabled = true;

        return this;
    }

    /** */
    public boolean isSystemTask() {
        return isSysTask;
    }

    /** */
    public TaskExecutionOptions asSystemTask() {
        isSysTask = true;

        return this;
    }

    /** */
    public boolean isPublicRequest() {
        return isPublicReq;
    }

    /**
     * Marks the task as initiated through the public API. It forces Ignite to perform security checks that prevent
     * access to Ignite internals before task execution. The main purpose of this flag is to separate the tasks that
     * were performed through the public and private APIs and to perform different security checks in each case.
     */
    public TaskExecutionOptions asPublicRequest() {
        isPublicReq = true;

        return this;
    }
}
