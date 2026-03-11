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

import java.util.BitSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;

public class TestMarshallableMessage implements MarshallableMessage {
    @Order(0)
    int iv;

    @Order(1)
    String sv;

    Object cstData;

    @Order(2)
    byte[] cstDataBytes;

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) {
        if (cstData != null && cstDataBytes == null) {
            try {
                cstDataBytes = U.marshal(marsh, cstData);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal custom data.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) {
        if (cstDataBytes != null && cstData == null) {
            try {
                cstData = U.unmarshal(marsh, cstDataBytes, clsLdr);

                cstDataBytes = null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal custom data.", e);
            }
        }
    }

    public short directType() {
        return 0;
    }
}
