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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionable;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Base for all messages in replicated cache.
 */
public abstract class GridDistributedBaseMessage extends GridCacheIdMessage implements GridCacheDeployable,
    GridCacheVersionable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Lock or transaction version. */
    @GridToStringInclude
    protected GridCacheVersion ver;

    /** */
    @GridToStringExclude
    private byte[] candsByIdxBytes;

    /** Committed versions with order higher than one for this message (needed for commit ordering). */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> committedVers;

    /** Rolled back versions with order higher than one for this message (needed for commit ordering). */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> rolledbackVers;

    /** Count of keys referenced in candidates array (needed only locally for optimization). */
    @GridToStringInclude
    @GridDirectTransient
    private int cnt;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    protected GridDistributedBaseMessage() {
        /* No-op. */
    }

    /**
     * @param cnt Count of keys references in list of candidates.
     * @param addDepInfo Deployment info flag.
     */
    protected GridDistributedBaseMessage(int cnt, boolean addDepInfo) {
        assert cnt >= 0;

        this.cnt = cnt;
        this.addDepInfo = addDepInfo;
    }

    /**
     * @param ver Either lock or transaction version.
     * @param cnt Key count.
     * @param addDepInfo Deployment info flag.
     */
    protected GridDistributedBaseMessage(GridCacheVersion ver, int cnt, boolean addDepInfo) {
        this(cnt, addDepInfo);

        assert ver != null;

        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return Version.
     */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @param ver Version.
     */
    public void version(GridCacheVersion ver) {
        this.ver = ver;
    }

    /**
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     */
    public void completedVersions(Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers) {
        this.committedVers = committedVers;
        this.rolledbackVers = rolledbackVers;
    }

    /**
     * @return Committed versions.
     */
    public Collection<GridCacheVersion> committedVersions() {
        return committedVers == null ? Collections.<GridCacheVersion>emptyList() : committedVers;
    }

    /**
     * @return Rolled back versions.
     */
    public Collection<GridCacheVersion> rolledbackVersions() {
        return rolledbackVers == null ? Collections.<GridCacheVersion>emptyList() : rolledbackVers;
    }

    /**
     * @return Count of keys referenced in candidates array (needed only locally for optimization).
     */
    int keysCount() {
        return cnt;
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
            case 4:
                if (!writer.writeByteArray("candsByIdxBytes", candsByIdxBytes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("committedVers", committedVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection("rolledbackVers", rolledbackVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("ver", ver))
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
            case 4:
                candsByIdxBytes = reader.readByteArray("candsByIdxBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                committedVers = reader.readCollection("committedVers", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                rolledbackVers = reader.readCollection("rolledbackVers", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                ver = reader.readMessage("ver");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDistributedBaseMessage.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedBaseMessage.class, this, "super", super.toString());
    }
}
