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

package org.apache.ignite.internal;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Request for cancelling tasks.
 */
public class GridTaskCancelRequest implements Message {
    /** */
    @Order(value = 0, method = "sessionId")
    private IgniteUuid sesId;

    /**
     * Default constructor.
     */
    public GridTaskCancelRequest() {
        // No-op.
    }

    /**
     * @param sesId Task session ID.
     */
    public GridTaskCancelRequest(IgniteUuid sesId) {
        assert sesId != null;

        this.sesId = sesId;
    }

    /**
     * Gets execution ID of task to be cancelled.
     *
     * @return Execution ID of task to be cancelled.
     */
    public IgniteUuid sessionId() {
        return sesId;
    }

    /**
     * @param sesId New session ID.
     */
    public void sessionId(IgniteUuid sesId) {
        this.sesId = sesId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTaskCancelRequest.class, this);
    }
}
