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

package org.apache.ignite.platform;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract task to test platform service.
 */
public abstract class AbstractPlatformServiceCallTask extends ComputeTaskAdapter<Object[], Object> {
    /** */
    @SuppressWarnings("unused")
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object[] arg)
            throws IgniteException {
        assert arg.length == 2;

        String srvcName = (String)arg[0];
        boolean loc = (Boolean)arg[1];

        Optional<ServiceDescriptor> desc = ignite.services().serviceDescriptors().stream()
                .filter(d -> d.name().equals(srvcName)).findAny();

        assert desc.isPresent();

        ClusterNode node;

        Set<UUID> srvTop = desc.get().topologySnapshot().keySet();

        if (loc) {
            UUID nodeId = F.rand(srvTop);

            node = ignite.cluster().node(nodeId);
        }
        else {
            node = ignite.cluster().nodes().stream().filter(n -> !srvTop.contains(n.id())).findAny().orElse(null);

            assert node != null;
        }

        return Collections.singletonMap(createJob(srvcName), node);
    }

    /** {@inheritDoc} */
    @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.get(0).getData();
    }

    /**
     * Create task job.
     *
     * @param svcName Service name
     * @return Instance of task job.
     */
    abstract ComputeJobAdapter createJob(String svcName);

    /** */
    protected abstract static class AbstractServiceCallJob extends ComputeJobAdapter {
        /** */
        @SuppressWarnings("unused")
        @IgniteInstanceResource
        protected transient Ignite ignite;

        /** */
        protected final String srvcName;

        /**
         * @param srvcName Service name.
         */
        protected AbstractServiceCallJob(String srvcName) {
            assert srvcName != null;

            this.srvcName = srvcName;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            try {
                runTest();

                return null;
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /**
         * Gets service proxy.
         */
        TestPlatformService serviceProxy() {
            return ignite.services().serviceProxy(srvcName, TestPlatformService.class, false);
        }

        /**
         * Test method to call platform service.
         */
        abstract void runTest();

        /**
         * Start thin client connected to current ignite instance.
         */
        IgniteClient startClient() {
            ClusterNode node = ignite.cluster().localNode();

            String addr = F.first(node.addresses()) + ":" + node.attribute(ClientListenerProcessor.CLIENT_LISTENER_PORT);

            return Ignition.startClient(new ClientConfiguration()
                .setAddresses(addr)
                .setBinaryConfiguration(ignite.configuration().getBinaryConfiguration())
            );
        }
    }

    /** */
    protected static void assertEquals(Object exp, Object res) throws IgniteException {
        if ((exp != null && !exp.equals(res)) || (res != null && !res.equals(exp)))
            throw new IgniteException(String.format("Expected equals to %s, got %s", exp, res));
    }

    /** */
    protected static void assertTrue(boolean res) {
        assertEquals(true, res);
    }

    /** */
    public interface TestPlatformService
    {
        /** */
        @PlatformServiceMethod("get_NodeId")
        UUID getNodeId();

        /** */
        @PlatformServiceMethod("get_GuidProp")
        UUID getGuidProp();

        /** */
        @PlatformServiceMethod("set_GuidProp")
        void setGuidProp(UUID val);

        /** */
        @PlatformServiceMethod("get_ValueProp")
        TestValue getValueProp();

        /** */
        @PlatformServiceMethod("set_ValueProp")
        void setValueProp(TestValue val);

        /** */
        @PlatformServiceMethod("ErrorMethod")
        void errorMethod();

        /** */
        @PlatformServiceMethod("AddOneToEach")
        TestValue[] addOneToEach(TestValue[] col);

        /** */
        @PlatformServiceMethod("AddOneToEachCollection")
        Collection<TestValue> addOneToEachCollection(Collection<TestValue> col);

        /** */
        @PlatformServiceMethod("AddOneToEachDictionary")
        Map<TestKey, TestValue> addOneToEachDictionary(Map<TestKey, TestValue> dict);

        /** */
        @PlatformServiceMethod("AddOne")
        BinarizableTestValue addOne(BinarizableTestValue val);
    }

    /** */
    public static class TestKey {
        /** */
        private final int id;

        /** */
        public TestKey(int id) {
            this.id = id;
        }

        /** */
        public int id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            return id == ((TestKey)o).id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id);
        }
    }

    /** */
    public static class TestValue {
        /** */
        protected int id;

        /** */
        protected String name;

        /** */
        public TestValue(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** */
        public int id() {
            return id;
        }

        /** */
        public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue val = (TestValue) o;

            return id == val.id && Objects.equals(name, val.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, name);
        }
    }

    /** */
    public static class BinarizableTestValue extends TestValue implements Binarylizable {
        /** */
        public BinarizableTestValue(int id, String name) {
            super(id, name);
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("id", id);
            writer.writeString("name", name);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            id = reader.readInt("id");
            name = reader.readString("name");
        }
    }
}
