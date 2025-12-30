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

package org.apache.ignite.spi.discovery.tcp.messages;

import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.processors.cluster.CacheMetricsMessage;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * We cannot directly reuse {@link CacheMetricsMessage} in Discovery as it is registered in a message factory of
 * Communication component and thus is unavailable in Discovery. We have to extend {@link CacheMetricsMessage} and
 * register this subclass in message factory of Discovery component.
 */
public class TcpDiscoveryCacheMetricsMessage extends CacheMetricsMessage {
    /** Constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryCacheMetricsMessage() {
        // No-op.
    }

    /** */
    public TcpDiscoveryCacheMetricsMessage(CacheMetrics m) {
        super(m);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -103;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryCacheMetricsMessage.class, this, "super", super.toString());
    }
}
