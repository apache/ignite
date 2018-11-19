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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Task that deploys a Java service.
 */
public class PlatformDeployServiceTask extends ComputeTaskAdapter<String, Object> {
    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable String serviceName) throws IgniteException {
        return Collections.singletonMap(new PlatformDeployServiceJob(serviceName), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class PlatformDeployServiceJob extends ComputeJobAdapter {
        /** Service name. */
        private final String serviceName;

        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Ctor.
         *
         * @param serviceName Service name.
         */
        private PlatformDeployServiceJob(String serviceName) {
            assert serviceName != null;
            this.serviceName = serviceName;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            ignite.services().deployNodeSingleton(serviceName, new PlatformTestService());

            return null;
        }
    }

    /**
     * Test service.
     */
    public static class PlatformTestService implements Service {
        /** */
        private boolean isCancelled;

        /** */
        private boolean isInitialized;

        /** */
        private boolean isExecuted;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            isCancelled = true;
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            isInitialized = true;
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            isExecuted = true;
        }

        /**
         * Returns a value indicating whether this service is cancelled.
         */
        public boolean isCancelled() {
            return isCancelled;
        }

        /**
         * Returns a value indicating whether this service is initialized.
         */
        public boolean isInitialized() {
            return isInitialized;
        }

        /**
         * Returns a value indicating whether this service is executed.
         */
        public boolean isExecuted() {
            return isExecuted;
        }

        /** */
        public byte test(byte arg) {
            return (byte) (arg + 1);
        }

        /** */
        public short test(short arg) {
            return (short) (arg + 1);
        }

        /** */
        public int test(int arg) {
            return arg + 1;
        }

        /** */
        public long test(long arg) {
            return arg + 1;
        }

        /** */
        public float test(float arg) {
            return arg + 1.5f;
        }

        /** */
        public double test(double arg) {
            return arg + 2.5;
        }

        /** */
        public boolean test(boolean arg) {
            return !arg;
        }

        /** */
        public char test(char arg) {
            return (char) (arg + 1);
        }

        /** */
        public String test(String arg) {
            return arg == null ? null : arg + "!";
        }

        /** */
        public Byte testWrapper(Byte arg) {
            return arg == null ? null : (byte) (arg + 1);
        }

        /** */
        public Short testWrapper(Short arg) {
            return arg == null ? null : (short) (arg + 1);
        }

        /** */
        public Integer testWrapper(Integer arg) {
            return arg == null ? null : arg + 1;
        }

        /** */
        public Long testWrapper(Long arg) {
            return arg == null ? null : arg + 1;
        }

        /** */
        public Float testWrapper(Float arg) {
            return arg == null ? null : arg + 1.5f;
        }

        /** */
        public Double testWrapper(Double arg) {
            return arg == null ? null :  arg + 2.5;
        }

        /** */
        public Boolean testWrapper(Boolean arg) {
            return arg == null ? null :  !arg;
        }

        /** */
        public Character testWrapper(Character arg) {
            return arg == null ? null :  (char) (arg + 1);
        }

        /** */
        public byte[] testArray(byte[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public short[] testArray(short[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public int[] testArray(int[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public long[] testArray(long[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public double[] testArray(double[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public float[] testArray(float[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public String[] testArray(String[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public char[] testArray(char[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public boolean[] testArray(boolean[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] = !arg[i];

            return arg;
        }

        /** */
        public Integer testNull(Integer arg) {
            return arg == null ? null : arg + 1;
        }

        /** */
        public int testParams(Object... args) {
            return args.length;
        }

        /** */
        public int test(int x, String y) {
            return x + 1;
        }

        /** */
        public int test(String x, int y) {
            return y + 1;
        }

        /** */
        public PlatformComputeBinarizable testBinarizable(PlatformComputeBinarizable arg) {
            return arg == null ? null : new PlatformComputeBinarizable(arg.field + 1);
        }

        /** */
        public Object[] testBinarizableArray(Object[] arg) {
            if (arg == null)
                return null;

            for (int i = 0; i < arg.length; i++)
                arg[i] = arg[i] == null
                    ? null
                    : new PlatformComputeBinarizable(((PlatformComputeBinarizable)arg[i]).field + 1);

            return arg;
        }

        /** */
        public Collection testBinarizableCollection(Collection arg) {
            if (arg == null)
                return null;

            Collection<PlatformComputeBinarizable> res = new ArrayList<>(arg.size());

            for(Object x : arg)
                res.add(new PlatformComputeBinarizable(((PlatformComputeBinarizable)x).field + 1));

            return res;
        }

        /** */
        public BinaryObject testBinaryObject(BinaryObject o) {
            if (o == null)
                return null;

            return o.toBuilder().setField("field", 15).build();
        }
    }
}
