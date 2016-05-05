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

package org.apache.ignite.internal.visor.compute;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * Task to run Visor tasks through http REST.
 */
@GridInternal
public class VisorGatewayTask implements ComputeTask<Object[], Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Auto-injected grid instance. */
    @IgniteInstanceResource
    protected transient IgniteEx ignite;

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object[] args) throws IgniteException {
        assert args != null;
        assert args.length >= 2;

        return Collections.singletonMap(new VisorGatewayJob(args), ignite.localNode());
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res,
        List<ComputeJobResult> rcvd) throws IgniteException {
        // Task should handle exceptions in reduce method.
        return ComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
        assert results.size() == 1;

        ComputeJobResult res = F.first(results);

        assert res != null;

        IgniteException ex = res.getException();

        if (ex != null)
            throw ex;

        return res.getData();
    }

    /**
     * Job to run Visor tasks through http REST.
     */
    private static class VisorGatewayJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        protected transient IgniteEx ignite;

        /** Arguments count. */
        private final int argsCnt;

        /**
         * Create job with specified argument.
         *
         * @param args Job argument.
         */
        VisorGatewayJob(@Nullable Object[] args) {
            super(args);

            assert args != null;

            argsCnt = args.length;
        }

        /**
         * Cast from string representation to target class.
         *
         * @param cls Target class.
         * @param val String representation.
         * @return Object constructed from string.
         */
        private static Object toObject(Class cls, String val) {
            if (val == null || String.class == cls)
                return val;

            if (Boolean.class == cls || Boolean.TYPE == cls)
                return Boolean.parseBoolean(val);

            if (Integer.class == cls || Integer.TYPE == cls)
                return Integer.parseInt(val);

            if (Long.class == cls || Long.TYPE == cls)
                return Long.parseLong(val);

            if (UUID.class == cls)
                return UUID.fromString(val);

            if (IgniteUuid.class == cls)
                return IgniteUuid.fromString(val);

            if (Byte.class == cls || Byte.TYPE == cls)
                return Byte.parseByte(val);

            if (Short.class == cls || Short.TYPE == cls)
                return Short.parseShort(val);

            if (Float.class == cls || Float.TYPE == cls)
                return Float.parseFloat(val);

            if (Double.class == cls || Double.TYPE == cls)
                return Double.parseDouble(val);

            if (BigDecimal.class == cls)
                return new BigDecimal(val);

            return val;
        }

        /**
         * Check if class is not a complex bean.
         *
         * @param cls Target class.
         * @return {@code True} if class is primitive or build-in java type or IgniteUuid.
         */
        private static boolean isSimpleObject(Class cls) {
            return cls.isPrimitive() || cls.getName().startsWith("java.") ||  IgniteUuid.class == cls;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            String nidsArg = argument(0);
            String taskName = argument(1);

            Object jobArgs = null;

            if (argsCnt > 2) {
                String argClsName = argument(2);

                assert argClsName != null;

                try {
                    Class<?> argCls = Class.forName(argClsName);

                    if (isSimpleObject(argCls)) {
                        String val = argument(3);

                        jobArgs = toObject(argCls, val);
                    }
                    else if (argCls == Collection.class) {
                        String colClsName = argument(3);

                        assert colClsName != null;

                        Class<?> colCls = Class.forName(colClsName);

                        Collection<Object> col = new ArrayList<>();

                        for (int i = 4; i < argsCnt; i++) {
                            String val = argument(i);

                            col.add(toObject(colCls, val));
                        }

                        jobArgs = col;
                    }
                    else {
                        int beanArgsCnt = argsCnt - 3;

                        for (Constructor ctor : argCls.getDeclaredConstructors()) {
                            Class[] types = ctor.getParameterTypes();

                            if (types.length == beanArgsCnt) {
                                Object[] initargs = new Object[beanArgsCnt];

                                for (int i = 0; i < beanArgsCnt; i++) {
                                    String val = argument(i + 3);

                                    initargs[i] = toObject(types[i], val);
                                }

                                jobArgs = ctor.newInstance(initargs);

                                break;
                            }
                        }
                    }
                }
                catch (Exception e) {
                    throw new IgniteException("Failed to construct task argument", e);
                }
            }

            final Collection<UUID> nids;

            if (nidsArg == null || nidsArg.isEmpty() || nidsArg.equals("null")) {
                Collection<ClusterNode> nodes = ignite.cluster().nodes();

                nids = new ArrayList<>(nodes.size());

                for (ClusterNode node : nodes)
                    nids.add(node.id());
            }
            else {
                String[] items = nidsArg.split(",");

                nids = new ArrayList<>(items.length);

                for (String item : items) {
                    try {
                        nids.add(UUID.fromString(item));
                    } catch (IllegalArgumentException ignore) {
                        // No-op.
                    }
                }
            }

            return ignite.compute().execute(taskName, new VisorTaskArgument<>(nids, jobArgs, false));
        }
    }
}
