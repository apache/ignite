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

package org.apache.ignite.internal.managers.encryption;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.encryption.EncryptionKey;
import org.apache.ignite.encryption.EncryptionSpi;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.ENCRYPTION_MGR;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_ENCRYPTION_MASTER_KEY_DIGEST;

/**
 * Manages cache encryptor.
 */
public class GridEncryptionManager extends GridManagerAdapter<EncryptionSpi> {
    /**
     * @param ctx Kernal context.
     */
    public GridEncryptionManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getEncryptionSpi());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        startSpi();

        ctx.addNodeAttribute(ATTR_ENCRYPTION_MASTER_KEY_DIGEST, getSpi().masterKeyDigest());

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        IgniteNodeValidationResult res = super.validateNode(node, discoData);

        if (res != null)
            return res;

        if (!discoData.hasJoiningNodeData())
            return validateNode(node);

        Map<Integer, byte[]> encKeysFromJoiningNode = (Map<Integer, byte[]>)discoData.joiningNodeData();

        if (F.isEmpty(encKeysFromJoiningNode))
            return validateNode(node);

        for (Map.Entry<Integer, byte[]> entry : encKeysFromJoiningNode.entrySet()) {
            EncryptionKey locEncKey = ctx.cache().context().database().groupKey(entry.getKey());

            if (locEncKey == null)
                continue;

            if (Arrays.equals(getSpi().encryptKey(locEncKey), entry.getValue()))
                continue;

            return new IgniteNodeValidationResult(ctx.localNodeId(),
                "Cache key differs! Node join is rejected. [nodeId=" + node.id() + "]",
                "Cache key differs! Node join is rejected.");
        }

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        IgniteNodeValidationResult res = super.validateNode(node);

        if (res != null)
            return res;

        byte[] lclMkDig = getSpi().masterKeyDigest();

        byte[] rmtMkDig = node.attribute(ATTR_ENCRYPTION_MASTER_KEY_DIGEST);

        if (Arrays.equals(lclMkDig, rmtMkDig))
            return null;

        return new IgniteNodeValidationResult(ctx.localNodeId(),
            "Master key digest differs! Node join is rejected. [nodeId=" + node.id() + "]",
            "Master key digest differs! Node join is rejected.");
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        Map<Integer, EncryptionKey<?>> encKeys = ctx.cache().context().database().encryptionKeys();

        if (F.isEmpty(encKeys))
            return;

        HashMap<Integer, byte[]> keysToSend = new HashMap<>();

        for (Map.Entry<Integer, EncryptionKey<?>> entry : encKeys.entrySet())
            keysToSend.put(entry.getKey(), getSpi().encryptKey(entry.getValue()));

        dataBag.addJoiningNodeData(ENCRYPTION_MGR.ordinal(), keysToSend);
    }

    /** {@inheritDoc} */
    public DiscoveryDataExchangeType discoveryDataType() {
        return ENCRYPTION_MGR;
    }
}
