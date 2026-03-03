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
    @Order(0)
    IgniteUuid sesId;

    /** */
    @Order(1)
    @GridToStringInclude(sensitive = true)
    String key;

    /** */
    @Order(2)
    String cpSpi;

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
     * @return Checkpoint key.
     */
    public String key() {
        return key;
    }

    /**
     * @return Checkpoint SPI.
     */
    public String checkpointSpi() {
        return cpSpi;
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
