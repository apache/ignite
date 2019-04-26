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

package org.apache.ignite.internal.processors.monitoring.lists;

/**
 *
 */
public class ComputeTaskInfo {
    private String taskClasName;

    private long startTime;

    private long timeout;

    private String execName;

    public ComputeTaskInfo(String taskClasName, long startTime, long timeout, String execName) {
        this.taskClasName = taskClasName;
        this.startTime = startTime;
        this.timeout = timeout;
        this.execName = execName;
    }

    public String getTaskClasName() {
        return taskClasName;
    }

    public void setTaskClasName(String taskClasName) {
        this.taskClasName = taskClasName;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getExecName() {
        return execName;
    }

    public void setExecName(String execName) {
        this.execName = execName;
    }
}
