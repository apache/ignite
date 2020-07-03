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

package org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.persistence;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.AbstractNodeJoinTemplate;
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.join.JoinInActiveNodeToActiveCluster;

/**
 *
 */
public class JoinInActiveNodeToActiveClusterWithPersistence extends JoinInActiveNodeToActiveCluster {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration cfg(String name) throws Exception {
        return persistentCfg(super.cfg(name));
    }

    private AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder persistent(AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder b) {
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

    @Override public AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder withOutConfigurationTemplate() throws Exception {
        AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder b = persistent(super.withOutConfigurationTemplate());

        b.afterActivate(b.checkCacheOnlySystem());

        return b;
    }

    @Override public AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder joinClientWithOutConfigurationTemplate() throws Exception {
        AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder b = persistent(super.joinClientWithOutConfigurationTemplate());

        b.afterActivate(b.checkCacheOnlySystem());

        return b;
    }

    @Override public AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationInClusterTemplate() throws Exception {
        return persistent(super.joinClientStaticCacheConfigurationInClusterTemplate());
    }

    @Override public AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationDifferentOnBothTemplate() throws Exception {
        return persistent(super.joinClientStaticCacheConfigurationDifferentOnBothTemplate());
    }

    @Override public AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder staticCacheConfigurationOnJoinTemplate() throws Exception {
        return persistent(super.staticCacheConfigurationOnJoinTemplate());
    }

    @Override public AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder staticCacheConfigurationInClusterTemplate() throws Exception {
        return persistent(super.staticCacheConfigurationInClusterTemplate());
    }

    @Override public AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder staticCacheConfigurationSameOnBothTemplate() throws Exception {
        return persistent(super.staticCacheConfigurationSameOnBothTemplate());
    }

    @Override public AbstractNodeJoinTemplate.JoinNodeTestPlanBuilder staticCacheConfigurationDifferentOnBothTemplate() throws Exception {
        return persistent(super.staticCacheConfigurationDifferentOnBothTemplate());
    }
}
