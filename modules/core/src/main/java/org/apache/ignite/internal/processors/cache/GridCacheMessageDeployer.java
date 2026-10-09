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
import org.apache.ignite.internal.managers.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Per-message deployer generated for a {@link GridCacheMessage} whose fields need deployment: the generated
 * {@code <Message>Deployer} deploys them via {@code GridCacheMessage}'s public {@code deploy*} methods. The static
 * {@link #deploy(MessageFactory, GridCacheMessage, GridCacheSharedContext)} resolves the deployer from the message
 * factory and dispatches, mirroring the static {@code MessageMarshaller#marshal}.
 */
public interface GridCacheMessageDeployer<M extends GridCacheMessage> {
    /** Deploys all deployable fields of {@code msg}. */
    void deploy(M msg, GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException;

    /**
     * Resolves {@code msg}'s deployer from the message factory and deploys the message's fields. A no-op when the
     * message has no deployer, or {@code msg} is {@code null} — generated deployers delegate nested message fields
     * here without null checks. Called both when sending a message and from a generated deployer for a nested message.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void deploy(MessageFactory factory, @Nullable GridCacheMessage msg, GridCacheSharedContext<?, ?> ctx)
        throws IgniteCheckedException {
        if (msg == null)
            return;

        // Deployment info is collected only when peer class loading may need it; skip the field scans otherwise.
        if (!msg.addDeploymentInfo() && !ctx.deploymentEnabled())
            return;

        GridCacheMessageDeployer deployer = ((IgniteMessageFactory)factory).deployer(msg.directType());

        if (deployer != null)
            deployer.deploy(msg, ctx);
    }
}
