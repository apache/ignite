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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.processors.platform.PlatformNativeException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;

/** */
public class PlatformServiceCallTask extends ComputeTaskAdapter<String, Object> {
    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String srvcName)
            throws IgniteException {
        return Collections.singletonMap(new PlatformServiceCallJob(srvcName), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.get(0).getData();
    }

    /** */
    private static class PlatformServiceCallJob extends ComputeJobAdapter {
        /** */
        private final String srvcName;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * @param srvcName Service name.
         */
        private PlatformServiceCallJob(String srvcName) {
            assert srvcName != null;
            this.srvcName = srvcName;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            TestPlatformService srv = ignite.services().serviceProxy(srvcName, TestPlatformService.class, false);

            UUID nodeId = srv.get_NodeId();
            assertTrue(ignite.cluster().nodes().stream().anyMatch(n -> n.id().equals(nodeId)));

            UUID expUuid = UUID.randomUUID();
            srv.set_GuidProp(expUuid);
            assertEquals(expUuid, srv.get_GuidProp());

            TestValue exp = new TestValue(1, "test");
            srv.set_ValueProp(exp);
            assertEquals(exp, srv.get_ValueProp());

            try {
                srv.ErrorMethod();

                throw new RuntimeException("Expected exception, but invocation was success");
            }
            catch (IgniteException e) {
                assertTrue(PlatformNativeException.class.isAssignableFrom(e.getCause().getClass()));

                PlatformNativeException nativeEx = (PlatformNativeException)e.getCause();

                assertTrue(nativeEx.toString().contains("Failed method"));
            }

            return null;
        }
    }

    /** */
    private static void assertEquals(Object exp, Object res) throws IgniteException {
        if ((exp != null && !exp.equals(res)) || (res != null && !res.equals(exp)))
            throw new IgniteException(String.format("Expected equals to %s, got %s", exp, res));
    }

    /** */
    private static void assertTrue(boolean res) {
        assertEquals(true, res);
    }


    /** */
    public interface TestPlatformService
    {
        /** */
        UUID get_NodeId();

        /** */
        UUID get_GuidProp();

        /** */
        void set_GuidProp(UUID val);

        /** */
        TestValue get_ValueProp();

        /** */
        void set_ValueProp(TestValue val);

        /** */
        void ErrorMethod();
    }

    /** */
    public static class TestValue
    {
        /** */
        private final int id;

        /** */
        private final String name;

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
}
