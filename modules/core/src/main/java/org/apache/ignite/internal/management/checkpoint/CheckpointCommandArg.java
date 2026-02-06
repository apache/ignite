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

package org.apache.ignite.internal.management.checkpoint;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;

/** Checkpoint command arguments. */
public class CheckpointCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(value = 0)
    @Argument(description = "Reason (visible in logs)", optional = true)
    String reason;

    /** */
    @Order(value = 1)
    @Argument(description = "Wait for checkpoint to finish", optional = true)
    boolean waitForFinish;

    /** */
    @Order(value = 2)
    @Argument(description = "Timeout in milliseconds", optional = true)
    long timeout = -1;

    /** */
    public String reason() {
        return reason;
    }

    /** */
    public void reason(String reason) {
        this.reason = reason;
    }

    /** */
    public boolean waitForFinish() {
        return waitForFinish;
    }

    /** */
    public void waitForFinish(boolean waitForFinish) {
        this.waitForFinish = waitForFinish;
    }

    /** */
    public long timeout() {
        return timeout;
    }

    /** */
    public void timeout(long timeout) {
        this.timeout = timeout;
    }
}
