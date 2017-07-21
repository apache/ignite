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

package org.apache.ignite.p2p;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.GridTestIoUtils;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.lang.IgniteProductVersion.fromString;

/**
 * P2P test.
 */
@GridCommonTest(group = "P2P")
public class GridP2PClassLoadingSelfTest extends GridCommonAbstractTest {
    /** */
    private final ClassLoader tstClsLdr;

    /** */
    public GridP2PClassLoadingSelfTest() {
        super(/*start grid*/false);

        tstClsLdr = new GridTestClassLoader(
            Collections.singletonMap("org/apache/ignite/p2p/p2p.properties", "resource=loaded"),
            GridP2PTestTask.class.getName(),
            GridP2PTestJob.class.getName()
        );
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"serial", "ConstantConditions"})
    public void testClassLoading() throws Exception {
        ComputeTask<?, ?> task = (ComputeTask<?, ?>)tstClsLdr.loadClass(GridP2PTestTask.class.getName()).newInstance();

        byte[] rawTask = GridTestIoUtils.serializeJdk(task);

        ComputeTask<Object, Integer> res = GridTestIoUtils.deserializeJdk(rawTask, tstClsLdr);

        ClusterNode fakeNode = new TestGridNode();

        List<ClusterNode> nodes = Collections.singletonList(fakeNode);

        ComputeJob p2pJob = res.map(nodes, 1).entrySet().iterator().next().getKey();

        assert p2pJob.getClass().getClassLoader() instanceof GridTestClassLoader : "Class loader = "
            + res.getClass().getClassLoader();
    }

    /** */
    private static class TestGridNode extends GridMetadataAwareAdapter implements ClusterNode {
        /** */
        private static AtomicInteger consistentIdCtr = new AtomicInteger();

        /** */
        private UUID nodeId = UUID.randomUUID();

        /** */
        private Object consistentId = consistentIdCtr.incrementAndGet();

        /** {@inheritDoc} */
        @Override public long order() {
            return -1;
        }

        /** {@inheritDoc} */
        @Override public IgniteProductVersion version() {
            return fromString("99.99.99");
        }

        /** {@inheritDoc} */
        @Override public UUID id() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public Object consistentId() {
            return consistentId;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Nullable @Override public <T> T attribute(String name) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public ClusterMetrics metrics() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<String, Object> attributes() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> addresses() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isLocal() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isDaemon() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isClient() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> hostNames() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return F.eqNodes(this, o);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id().hashCode();
        }
    }
}