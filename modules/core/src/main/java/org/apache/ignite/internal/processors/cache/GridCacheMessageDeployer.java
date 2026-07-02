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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Per-message deployer. A generated {@code <Msg>Deployer} implements {@link #deploy(GridCacheMessage, GridCacheSharedContext)}
 * to deploy the message's fields (via {@code GridCacheMessage}'s public {@code deploy*} methods). The static
 * {@link #deploy(MessageFactory, GridCacheMessage, GridCacheSharedContext)} is the factory-resolving entry point,
 * mirroring the static {@code MessageMarshaller#marshal}.
 */
public interface GridCacheMessageDeployer<M extends GridCacheMessage> {
    /** Deploys all deployable fields of {@code msg}. */
    void deploy(M msg, GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException;

    /**
     * Deploys {@code msg} through its factory-registered deployer (a no-op when {@code msg} is
     * {@code null} — e.g. an absent nested message — or the message has no registered deployer). Single entry point
     * for message deployment: called both by message-sending code and by a generated deployer delegating to a nested
     * message. Mirrors the static {@code MessageMarshaller#marshal}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void deploy(MessageFactory factory, @Nullable GridCacheMessage msg, GridCacheSharedContext<?, ?> ctx)
        throws IgniteCheckedException {
        if (msg == null)
            return;

        // Deployment info is collected only when peer class loading may need it; skip the field scans otherwise.
        if (!msg.addDeploymentInfo() && !ctx.deploymentEnabled())
            return;

        GridCacheMessageDeployer deployer = factory.deployer(msg.directType());

        if (deployer != null)
            deployer.deploy(msg, ctx);
    }
}
