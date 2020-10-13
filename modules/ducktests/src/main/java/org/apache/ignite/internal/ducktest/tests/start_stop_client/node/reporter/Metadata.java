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

package org.apache.ignite.internal.ducktest.tests.start_stop_client.node.reporter;

/**
 * Support information about current run
 * */
public class Metadata {
    /** */
    private String actionName = "none";

    /** */
    private int threads = -1;

    /** */
    private Long startTime = -1l;

    /** */
    public String getActionName() {
        return actionName;
    }

    /** */
    public void setActionName(String actionName) {
        this.actionName = actionName;
    }

    /** */
    public int getThreads() {
        return threads;
    }

    /** */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /** */
    public Long getStartTime() {
        return startTime;
    }

    /** */
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }
}
