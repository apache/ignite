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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Deferred dht atomic update response.
 */
public class GridDhtAtomicDeferredUpdateResponse extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** ACK future versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> futVers;

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridDhtAtomicDeferredUpdateResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param futVers Future versions.
     * @param addDepInfo Deployment info.
     */
    public GridDhtAtomicDeferredUpdateResponse(int cacheId, Collection<GridCacheVersion> futVers, boolean addDepInfo) {
        assert !F.isEmpty(futVers);

        this.cacheId = cacheId;
        this.futVers = futVers;
        this.addDepInfo = addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return List of ACKed future versions.
     */
    public Collection<GridCacheVersion> futureVersions() {
        return futVers;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.atomicMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeCollection("futVers", futVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                futVers = reader.readCollection("futVers", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAtomicDeferredUpdateResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 37;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }
}
