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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * The data object describes a version of key stored at a node.
 */
public class VersionedKey extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node id. */
    private UUID nodeId;

    /** Key. */
    private KeyCacheObject key;

    /** Version. */
    private GridCacheVersion ver;

    /**
     * Default constructor.
     */
    public VersionedKey() {
        // No-op
    }

    /**
     * @param nodeId Node id.
     * @param key Key.
     * @param ver Version.
     */
    public VersionedKey(UUID nodeId, KeyCacheObject key, GridCacheVersion ver) {
        this.nodeId = nodeId;
        this.key = key;
        this.ver = ver;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Key.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Write version of current entry.
     */
    public GridCacheVersion ver() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(nodeId);
        out.writeObject(key);
        out.writeObject(ver);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = (UUID)in.readObject();
        key = (KeyCacheObject)in.readObject();
        ver = (GridCacheVersion)in.readObject();
    }
}
