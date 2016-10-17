/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.ignite.internal.processors.security;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.plugin.*;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.jetbrains.annotations.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class IgniteSecurityPluginProvider implements PluginProvider {
    /** {@inheritDoc} */
    @Override public String name() {
        return "security plugin test";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {

    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {

    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {

    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {

    }

    /** {@inheritDoc} */
    @Nullable @Override public Object createComponent(PluginContext ctx, Class cls) {
        if (cls.equals(GridSecurityProcessor.class)) {
            IgniteEx grid = (IgniteEx) ctx.grid();

            Map<String, ?> attr = grid.configuration().getUserAttributes();

            SecurityCredentials sc = (SecurityCredentials) attr.get("crd");
            AtomicInteger authCnt = (AtomicInteger) attr.get("selfCnt");
            Map<UUID,List<UUID>> rmAuth = (Map<UUID, List<UUID>>) attr.get("rmAuth");
            Boolean global= (Boolean) attr.get("global");

            Map<SecurityCredentials, GridSecurityProcessorSelfTest.TestSecurityPermissionSet> permsMap =
                    (Map<SecurityCredentials, GridSecurityProcessorSelfTest.TestSecurityPermissionSet>) attr.get("permsMap");

            grid.context().addNodeAttribute(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, sc);

            return new GridSecurityProcessorSelfTest.GridTestSecurityProcessor(
                    grid.context(), authCnt, rmAuth, global, permsMap
            );
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgnitePlugin plugin() {
        return new IgnitePlugin() {

        };
    }
}
