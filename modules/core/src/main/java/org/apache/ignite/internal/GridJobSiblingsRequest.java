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
 * Job siblings request.
 */
public class GridJobSiblingsRequest implements Message {
    /** */
    @Order(value = 0, method = "sessionId")
    private IgniteUuid sesId;

    /** */
    @Order(1)
    private long topicId;

    /**
     * Empty constructor.
     */
    public GridJobSiblingsRequest() {
        // No-op.
    }

    /**
     * @param sesId Session ID.
     * @param topicId Topic ID.
     */
    public GridJobSiblingsRequest(IgniteUuid sesId, long topicId) {
        assert sesId != null;

        this.sesId = sesId;
        this.topicId = topicId;
    }

    /**
     * @return Session ID.
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

    /**
     * @return Topic ID.
     */
    public long topicId() {
        return topicId;
    }

    /**
     * @param topicId New topic ID.
     */
    public void topicId(long topicId) {
        this.topicId = topicId;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobSiblingsRequest.class, this);
    }
}
