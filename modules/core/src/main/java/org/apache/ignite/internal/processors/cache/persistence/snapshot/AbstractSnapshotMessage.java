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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
abstract class AbstractSnapshotMessage implements Message {
    /** Unique message ID. */
    @Order(0)
    private String id;

    /**
     * Empty constructor.
     */
    protected AbstractSnapshotMessage() {
        // No-op.
    }

    /**
     * @param id Unique message ID.
     */
    protected AbstractSnapshotMessage(String id) {
        assert U.alphanumericUnderscore(id) : id;

        this.id = id;
    }

    /**
     * @return Unique message ID.
     */
    public String id() {
        return id;
    }

    /**
     * @param id Unique message ID.
     */
    public void id(String id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractSnapshotMessage.class, this);
    }
}
