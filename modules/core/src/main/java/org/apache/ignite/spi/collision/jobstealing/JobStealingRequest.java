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

package org.apache.ignite.spi.collision.jobstealing;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Job stealing request.
 */
public class JobStealingRequest implements Message {
    /** Delta. */
    @Order(0)
    private int delta;

    /**
     * Empty constructor.
     */
    public JobStealingRequest() {
        // No-op.
    }

    /**
     * @param delta Delta.
     */
    JobStealingRequest(int delta) {
        this.delta = delta;
    }

    /**
     * @return Delta.
     */
    public int delta() {
        return delta;
    }

    /**
     * @param delta New delta.
     */
    public void delta(int delta) {
        this.delta = delta;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 82;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JobStealingRequest.class, this);
    }
}
