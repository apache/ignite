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

package org.apache.ignite.internal.processors.cache.persistence.standbycluster.join;

import org.apache.ignite.internal.processors.cache.persistence.standbycluster.AbstractNodeJoinTemplate;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class JoinActiveNodeToInActiveCluster extends AbstractNodeJoinTemplate {
    /** {@inheritDoc} */
    @Override public JoinNodeTestPlanBuilder withOutConfigurationTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = builder();

        b.clusterConfiguration(
            cfg(name(0)).setActiveOnStart(false),
            cfg(name(1)).setActiveOnStart(false),
            cfg(name(2)).setActiveOnStart(false)
        ).afterClusterStarted(
            b.checkCacheEmpty()
        ).nodeConfiguration(
            cfg(name(3))
                .setActiveOnStart(true)
        ).afterNodeJoin(
            b.checkCacheEmpty()
        ).stateAfterJoin(
            false
        ).afterActivate(
            b.checkCacheOnlySystem()
        );

        return b;
    }

    /** {@inheritDoc} */
    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationOnJoinTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = builder();

        b.clusterConfiguration(
            cfg(name(0)).setActiveOnStart(false),
            cfg(name(1)).setActiveOnStart(false),
            cfg(name(2)).setActiveOnStart(false)
        ).afterClusterStarted(
            b.checkCacheEmpty()
        ).nodeConfiguration(
            cfg(name(3))
                .setActiveOnStart(true)
                .setCacheConfiguration(allCacheConfigurations())
        ).afterNodeJoin(
            b.checkCacheEmpty()
        ).stateAfterJoin(
            false
        ).afterActivate(
            b.checkCacheNotEmpty()
        );

        return b;
    }

    /** {@inheritDoc} */
    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationInClusterTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = builder();

        b.clusterConfiguration(
            cfg(name(0))
                .setActiveOnStart(false)
                .setCacheConfiguration(allCacheConfigurations()),
            cfg(name(1)).setActiveOnStart(false),
            cfg(name(2)).setActiveOnStart(false)
        ).afterClusterStarted(
            b.checkCacheEmpty()
        ).nodeConfiguration(
            cfg(name(3)).setActiveOnStart(true)
        ).afterNodeJoin(
            b.checkCacheEmpty()
        ).stateAfterJoin(
            false
        ).afterActivate(
            b.checkCacheNotEmpty()
        );

        return b;
    }

    /** {@inheritDoc} */
    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationSameOnBothTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = builder();

        b.clusterConfiguration(
            cfg(name(0))
                .setActiveOnStart(false)
                .setCacheConfiguration(allCacheConfigurations()),
            cfg(name(1)).setActiveOnStart(false),
            cfg(name(2)).setActiveOnStart(false)
        ).afterClusterStarted(
            b.checkCacheEmpty()
        ).nodeConfiguration(
            cfg(name(3))
                .setActiveOnStart(true)
                .setCacheConfiguration(allCacheConfigurations())
        ).afterNodeJoin(
            b.checkCacheEmpty()
        ).stateAfterJoin(
            false
        ).afterActivate(
            b.checkCacheNotEmpty()
        );
        return b;
    }

    /** {@inheritDoc} */
    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationDifferentOnBothTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = builder();

        b.clusterConfiguration(
            cfg(name(0))
                .setActiveOnStart(false)
                .setCacheConfiguration(atomicCfg()),
            cfg(name(1)).setActiveOnStart(false),
            cfg(name(2)).setActiveOnStart(false)
        ).afterClusterStarted(
            b.checkCacheEmpty()
        ).nodeConfiguration(
            cfg(name(3))
                .setActiveOnStart(true)
                .setCacheConfiguration(transactionCfg())
        ).afterNodeJoin(
            b.checkCacheEmpty()
        ).stateAfterJoin(
            false
        ).afterActivate(
            b.checkCacheNotEmpty()
        );

        return b;
    }

    // Server node join.

    /** {@inheritDoc} */
    @Test
    @Override public void testJoinWithOutConfiguration() throws Exception {
        withOutConfigurationTemplate().execute();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testStaticCacheConfigurationOnJoin() throws Exception {
        staticCacheConfigurationOnJoinTemplate().execute();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testStaticCacheConfigurationInCluster() throws Exception {
        staticCacheConfigurationInClusterTemplate().execute();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testStaticCacheConfigurationSameOnBoth() throws Exception {
        staticCacheConfigurationSameOnBothTemplate().execute();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testStaticCacheConfigurationDifferentOnBoth() throws Exception {
        staticCacheConfigurationDifferentOnBothTemplate().execute();
    }

    // Client node join.

    /** {@inheritDoc} */
    @Test
    @Override public void testJoinClientWithOutConfiguration() throws Exception {
        joinClientWithOutConfigurationTemplate().execute();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testJoinClientStaticCacheConfigurationOnJoin() throws Exception {
        joinClientStaticCacheConfigurationOnJoinTemplate().execute();
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-5518")
    @Test
    @Override public void testJoinClientStaticCacheConfigurationInCluster() throws Exception {
        joinClientStaticCacheConfigurationInClusterTemplate().execute();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testJoinClientStaticCacheConfigurationSameOnBoth() throws Exception {
        joinClientStaticCacheConfigurationSameOnBothTemplate().execute();
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-5518")
    @Test
    @Override public void testJoinClientStaticCacheConfigurationDifferentOnBoth() throws Exception {
        joinClientStaticCacheConfigurationDifferentOnBothTemplate().execute();
    }

    @Override public JoinNodeTestPlanBuilder joinClientWithOutConfigurationTemplate() throws Exception {
        return withOutConfigurationTemplate().nodeConfiguration(setClient);
    }

    @Override public JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationOnJoinTemplate() throws Exception {
        return staticCacheConfigurationOnJoinTemplate().nodeConfiguration(setClient);
    }

    @Override public JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationInClusterTemplate() throws Exception {
        return staticCacheConfigurationInClusterTemplate().nodeConfiguration(setClient);
    }

    @Override public JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationDifferentOnBothTemplate() throws Exception {
        return staticCacheConfigurationDifferentOnBothTemplate().nodeConfiguration(setClient);
    }

    @Override public JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationSameOnBothTemplate() throws Exception {
        return staticCacheConfigurationSameOnBothTemplate().nodeConfiguration(setClient);
    }

}
