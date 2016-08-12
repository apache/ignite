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

package org.apache.ignite.internal.processors.cache.database.tree.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Utility to read and write {@link GridCacheVersion} instances.
 */
public class CacheVersionIO {
    /** Serialized size in bytes. */
    private static final int VER_SIZE_1 = 25;

    /**
     * @param ver Version.
     * @return Serialized size in bytes.
     */
    public static int size(GridCacheVersion ver) {
        assert ver != null;

        return VER_SIZE_1;
    }

    /**
     * @param buf Byte buffer.
     * @param ver Version to write.
     */
    public static void write(ByteBuffer buf, GridCacheVersion ver) {
        assert ver != null;

        byte protoVer = 1; // Version of serialization protocol.

        buf.put(protoVer);
        buf.putInt(ver.topologyVersion());
        buf.putInt(ver.nodeOrderAndDrIdRaw());
        buf.putLong(ver.globalTime());
        buf.putLong(ver.order());
    }

    private static byte checkProtocolVersion(byte protoVer) throws IgniteCheckedException {
        if (protoVer != 1)
            throw new IgniteCheckedException("Unsupported version: " + protoVer);

        return protoVer;
    }

    /**
     * Gets needed buffer size to read the whole version instance.
     * Does not change buffer position.
     *
     * @param buf Buffer.
     * @return Size of serialized version.
     * @throws IgniteCheckedException If failed.
     */
    public static int readSize(ByteBuffer buf) throws IgniteCheckedException {
        checkProtocolVersion(buf.get(buf.position()));

        return VER_SIZE_1;
    }

    /**
     * @param buf Byte buffer.
     * @return Version.
     * @throws IgniteCheckedException If failed.
     */
    public static GridCacheVersion read(ByteBuffer buf) throws IgniteCheckedException {
        checkProtocolVersion(buf.get());

        int topVer = buf.getInt();
        int nodeOrderDrId = buf.getInt();
        long globalTime = buf.getLong();
        long order = buf.getLong();

        return new GridCacheVersion(topVer, nodeOrderDrId, globalTime, order);
    }
}
