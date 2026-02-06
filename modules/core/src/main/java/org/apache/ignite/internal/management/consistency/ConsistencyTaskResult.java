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

package org.apache.ignite.internal.management.consistency;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/**
 *
 */
public class ConsistencyTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Result. */
    @Order(value = 0)
    String msg;

    /** Failed. */
    @Order(value = 1)
    boolean failed;

    /** Cancelled. */
    @Order(value = 2)
    boolean cancelled;

    /**
     * @return Result.
     */
    public String message() {
        return msg;
    }

    /**
     * @param res New result.
     */
    public void message(String res) {
        this.msg = res;
    }

    /**
     * @return Failed.
     */
    public boolean failed() {
        return failed;
    }

    /**
     * @param failed Failed.
     */
    public void failed(boolean failed) {
        this.failed = failed;
    }

    /**
     * @return Cancelled.
     */
    public boolean cancelled() {
        return cancelled;
    }

    /**
     * @param cancelled New cancelled.
     */
    public void cancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }
}
