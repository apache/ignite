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

package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Arguments required for adding near cache reader to entry.
 */
public class ReaderArguments {
    /** */
    private final UUID reader;

    /** */
    private final long msgId;

    /** */
    private final AffinityTopologyVersion topVer;

    /**
     * @param reader Near cache node ID.
     * @param msgId Message ID.
     * @param topVer Topology version.
     */
    public ReaderArguments(final UUID reader, final long msgId,
        final AffinityTopologyVersion topVer) {
        this.reader = reader;
        this.msgId = msgId;
        this.topVer = topVer;
    }

    /**
     * @return Reader node ID.
     */
    public UUID reader() {
        return reader;
    }

    /**
     * @return Message ID.
     */
    public long messageId() {
        return msgId;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ReaderArguments.class, this);
    }
}
