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

package org.apache.ignite.internal.util.distributed;

/**
 * Defines distributed processes.
 */
public enum DistributedProcesses {
    /** */
    MASTER_KEY_CHANGE_PREPARE(0),

    /** */
    MASTER_KEY_CHANGE_FINISH(1);

    /**
     * Unique process type identifier.
     */
    private final int procTypeId;

    /**
     * @param procId Process type ID.
     */
    DistributedProcesses(int procId) {
        this.procTypeId = procId;
    }

    /**
     * @return Process type ID.
     */
    public int processTypeId() {
        return procTypeId;
    }
}
