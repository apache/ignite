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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/** Snapshot operation prepare response. */
public class SnapshotRestoreOperationResponse implements MarshallableMessage {
    /** Cache configurations on local node. */
    private List<StoredCacheData> ccfgs;

    /** */
    @Order(0)
    byte[] ccfgsBytes;
    
    /** Snapshot metadata files on local node. */
    private List<SnapshotMetadata> metas;

    /** */
    @Order(1)
    byte[] metasBytes;

    /** Default constructor for {@link MessageFactory}. */
    public SnapshotRestoreOperationResponse() {
        // No-op.
    }

    /**
     * @param ccfgs Cache configurations on local node.
     * @param metas Snapshot metadata files on local node.
     */
    public SnapshotRestoreOperationResponse(
        Collection<StoredCacheData> ccfgs,
        Collection<SnapshotMetadata> metas
    ) {
        this.ccfgs = new ArrayList<>(ccfgs);
        this.metas = new ArrayList<>(metas);
    }

    /** */
    public List<StoredCacheData> cacheConfigurations() {
        return ccfgs;
    }

    /** */
    public List<SnapshotMetadata> metadata() {
        return metas;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        ccfgsBytes = U.marshal(marsh, ccfgs);
        metasBytes = U.marshal(marsh, metas);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        if (ccfgsBytes != null)
            ccfgs = U.unmarshal(marsh, ccfgsBytes, clsLdr);

        if (metasBytes != null)
            metas = U.unmarshal(marsh, metasBytes, clsLdr);
    }
    
    /** {@inheritDoc} */
    @Override public short directType() {
        return 525;
    }
}
