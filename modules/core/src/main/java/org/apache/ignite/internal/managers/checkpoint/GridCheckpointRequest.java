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

package org.apache.ignite.internal.managers.checkpoint;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * This class defines checkpoint request.
 */
public class GridCheckpointRequest implements Message {
    /** */
    @Order(value = 0, method = "sessionId")
    private IgniteUuid sesId;

    /** */
    @Order(1)
    @GridToStringInclude(sensitive = true)
    private String key;

    /** */
    @Order(value = 2, method = "checkpointSpi")
    private String cpSpi;

    /**
     * Empty constructor.
     */
    public GridCheckpointRequest() {
        // No-op.
    }

    /**
     * @param sesId Task session ID.
     * @param key Checkpoint key.
     * @param cpSpi Checkpoint SPI.
     */
    public GridCheckpointRequest(IgniteUuid sesId, String key, String cpSpi) {
        assert sesId != null;
        assert key != null;

        this.sesId = sesId;
        this.key = key;

        this.cpSpi = cpSpi == null || cpSpi.isEmpty() ? null : cpSpi;
    }

    /**
     * @return Session ID.
     */
    public IgniteUuid sessionId() {
        return sesId;
    }

    /**
     * @param sesId Session ID.
     */
    public void sessionId(IgniteUuid sesId) {
        this.sesId = sesId;
    }

    /**
     * @return Checkpoint key.
     */
    public String key() {
        return key;
    }

    /**
     * @param key Checkpoint key.
     */
    public void key(String key) {
        this.key = key;
    }

    /**
     * @return Checkpoint SPI.
     */
    public String checkpointSpi() {
        return cpSpi;
    }

    /**
     * @param cpSpi Checkpoint SPI.
     */
    public void checkpointSpi(String cpSpi) {
        this.cpSpi = cpSpi;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 7;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCheckpointRequest.class, this);
    }
}
