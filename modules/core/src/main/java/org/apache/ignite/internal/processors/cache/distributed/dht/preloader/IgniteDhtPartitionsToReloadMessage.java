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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.Serializable;
import java.util.Collection;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Message for partitions to reload. */
public class IgniteDhtPartitionsToReloadMessage implements Message, Serializable {
    /** Type code. */
    public static final short TYPE_CODE = 516;

    /** */
    private static final long serialVersionUID = 0L;

    /** Partitions. */
    @Order(value = 0, method = "partitions")
    private Collection<Integer> parts;

    /** Constructor. */
    public IgniteDhtPartitionsToReloadMessage() {
        // No-op.
    }

    /**
     * @param parts Partitions to reload.
     */
    public IgniteDhtPartitionsToReloadMessage(Collection<Integer> parts) {
        this.parts = parts;
    }

    /**
     * @return Partitions to reload.
     */
    public Collection<Integer> partitions() {
        return parts;
    }

    /**
     * @param parts Partitions to reload.
     */
    public void partitions(Collection<Integer> parts) {
        this.parts = parts;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
