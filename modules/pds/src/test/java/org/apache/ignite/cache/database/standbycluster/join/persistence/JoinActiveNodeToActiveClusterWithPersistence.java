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

package org.apache.ignite.cache.database.standbycluster.join.persistence;

import org.apache.ignite.cache.database.standbycluster.join.JoinActiveNodeToActiveCluster;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 *
 */
public class JoinActiveNodeToActiveClusterWithPersistence extends JoinActiveNodeToActiveCluster {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration cfg(String name) throws Exception {
        return persistentCfg(super.cfg(name));
    }

    private JoinNodeTestPlanBuilder persistent(JoinNodeTestPlanBuilder b) {
        b.afterClusterStarted(
            b.checkCacheEmpty()
        ).stateAfterJoin(
            false
        ).afterNodeJoin(
            b.checkCacheEmpty()
        ).afterActivate(
            b.checkCacheNotEmpty()
        );

        return b;
    }

    @Override public JoinNodeTestPlanBuilder withOutConfigurationTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = persistent(super.withOutConfigurationTemplate());

        b.afterActivate(b.checkCacheOnlySystem());

        return b;
    }

    @Override public JoinNodeTestPlanBuilder joinClientWithOutConfigurationTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = persistent(super.joinClientWithOutConfigurationTemplate());

        b.afterActivate(b.checkCacheOnlySystem());

        return b;
    }

    @Override public void testJoinWithOutConfiguration() throws Exception {
        withOutConfigurationTemplate().execute();
    }

    @Override public void testJoinClientWithOutConfiguration() throws Exception {
        joinClientWithOutConfigurationTemplate().execute();
    }

    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationOnJoinTemplate() throws Exception {
        return persistent(super.staticCacheConfigurationOnJoinTemplate());
    }

    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationInClusterTemplate() throws Exception {
        return persistent(super.staticCacheConfigurationInClusterTemplate());
    }

    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationSameOnBothTemplate() throws Exception {
        return persistent(super.staticCacheConfigurationSameOnBothTemplate());
    }

    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationDifferentOnBothTemplate() throws Exception {
        return persistent(super.staticCacheConfigurationDifferentOnBothTemplate());
    }

    @Override public JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationOnJoinTemplate() throws Exception {
        return persistent(super.joinClientStaticCacheConfigurationOnJoinTemplate());
    }

    @Override public JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationInClusterTemplate() throws Exception {
        return persistent(super.joinClientStaticCacheConfigurationInClusterTemplate());
    }

    @Override public JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationDifferentOnBothTemplate() throws Exception {
        return persistent(super.joinClientStaticCacheConfigurationDifferentOnBothTemplate());
    }
}
