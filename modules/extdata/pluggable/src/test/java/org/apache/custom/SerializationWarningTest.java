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

package org.apache.custom;

import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class SerializationWarningTest extends GridCommonAbstractTest {
    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog);
    }

    /** */
    @Test
    public void testDevSerializationWarning() throws Exception {
        LogListener lsnr = LogListener.matches("cannot be serialized").build();

        listeningLog.registerListener(lsnr);

        IgniteEx srv = startGrid(0);
        IgniteEx srv1 = startGrid(1);

        srv.compute().execute(TestManagementTask.class.getName(),
            new VisorTaskArgument<>(srv1.localNode().id(), new TestArg(), false));

        assertFalse(lsnr.check());

        srv.compute().execute(TestNotManagementTask.class.getName(), new TestExternalizableArg());

        assertTrue(lsnr.check());
    }

    /** */
    private static class TestManagementTask extends VisorOneNodeTask<TestArg, TestArg> {
        /** {@inheritDoc} */
        @Override protected VisorJob<TestArg, TestArg> job(TestArg arg) {
            return new VisorJob<TestArg, TestArg>(arg, false) {
                @Override protected TestArg run(@Nullable TestArg arg) throws IgniteException {
                    return new TestArg();
                }
            };
        }
    }

    /** */
    private static class TestArg extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        public TestArg() { }

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in) {
            // No-op.
        }
    }

    /** */
    private static class TestNotManagementTask extends ComputeTaskAdapter<TestExternalizableArg, TestExternalizableArg> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable TestExternalizableArg arg) throws IgniteException {
            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            for (ClusterNode node : subgrid) {
                map.put(new ComputeJobAdapter() {
                    @Override public Serializable execute() {
                        return arg;
                    }
                }, node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Nullable @Override public TestExternalizableArg reduce(List<ComputeJobResult> results) throws IgniteException {
            return results.get(0).getData();
        }
    }

    /** */
    private static class TestExternalizableArg implements Externalizable {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        public TestExternalizableArg() { }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) {
            // No-op.
        }
    }
}
