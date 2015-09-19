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

package org.apache.ignite.internal.processors.rest.request;

import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Grid task command request.
 */
public class GridRestTaskRequest extends GridRestRequest {
    /** Task name. */
    private String taskName;

    /** Task Id. */
    private String taskId;

    /** Parameters. */
    private List<Object> params;

    /** Asynchronous execution flag. */
    private boolean async;

    /** Timeout. */
    private long timeout;

    /**
     * @return Task name, if specified, {@code null} otherwise.
     */
    public String taskName() {
        return taskName;
    }

    /**
     * @param taskName Name of task for execution.
     */
    public void taskName(String taskName) {
        this.taskName = taskName;
    }

    /**
     * @return Task identifier, if specified, {@code null} otherwise.
     */
    public String taskId() {
        return taskId;
    }

    /**
     * @param taskId Task identifier.
     */
    public void taskId(String taskId) {
        this.taskId = taskId;
    }

    /**
     * @return Asynchronous execution flag.
     */
    public boolean async() {
        return async;
    }

    /**
     * @param async Asynchronous execution flag.
     */
    public void async(boolean async) {
        this.async = async;
    }

    /**
     * @return Task execution parameters.
     */
    public List<Object> params() {
        return params;
    }

    /**
     * @param params Task execution parameters.
     */
    public void params(List<Object> params) {
        this.params = params;
    }

    /**
     * @return Timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @param timeout Timeout.
     */
    public void timeout(long timeout) {
        this.timeout = timeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestTaskRequest.class, this, super.toString());
    }
}