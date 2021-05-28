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

package org.apache.ignite.network.internal.recovery.message;

import java.util.UUID;

public class HandshakeStartMessageImpl implements HandshakeStartMessage, HandshakeStartMessage.Builder {
    /** */
    private UUID launchId;

    /** */
    private String consistentId;

    /** {@inheritDoc} */
    @Override public UUID launchId() {
        return launchId;
    }

    /** {@inheritDoc} */
    @Override public String consistentId() {
        return consistentId;
    }

    /** {@inheritDoc} */
    @Override public Builder launchId(UUID launchId) {
        this.launchId = launchId;
        return this;
    }

    /** {@inheritDoc} */
    @Override public Builder consistentId(String consistentId) {
        this.consistentId = consistentId;
        return this;
    }

    /** {@inheritDoc} */
    @Override public HandshakeStartMessage build() {
        return this;
    }
}
