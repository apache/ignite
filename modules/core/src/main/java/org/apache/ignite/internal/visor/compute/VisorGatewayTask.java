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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
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

        /** Auto-inject job context. */
        @JobContextResource
        protected transient ComputeJobContext jobCtx;

        /** Arguments count. */
        private final int argsCnt;

        /** Future for spawned task. */
        private transient IgniteFuture fut;

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
         * Cast argument to target class.
         *
         * @param cls Class.
         * @param idx Argument index.
         */
        @Nullable private Object toObject(Class cls, int idx) throws ClassNotFoundException {
            String arg = argument(idx);

            if (cls == Collection.class || cls == Set.class) {
                Class<?> itemsCls = Class.forName(arg);

                Collection<Object> res = cls == Collection.class ? new ArrayList<>() : new HashSet<>();

                String items = argument(idx + 1);

                if (items != null) {
                    for (String item : items.split(";"))
                        res.add(toSimpleObject(itemsCls, item));
                }

                return res;
            }

            if (cls == IgniteBiTuple.class) {
                Class<?> keyCls = Class.forName(arg);

                String valClsName = argument(idx + 1);

                assert valClsName != null;

                Class<?> valCls = Class.forName(valClsName);

                return new IgniteBiTuple<>(toSimpleObject(keyCls, (String)argument(idx + 2)),
                    toSimpleObject(valCls, (String)argument(idx + 3)));
            }

            if (cls == Map.class) {
                Class<?> keyCls = Class.forName(arg);

                String valClsName = argument(idx + 1);

                assert valClsName != null;

                Class<?> valCls = Class.forName(valClsName);

                Map<Object, Object> res = new HashMap<>();

                String entries = argument(idx + 2);

                if (entries != null) {
                    for (String entry : entries.split(";")) {
                        if (entry.length() > 0) {
                            String[] values = entry.split("=");

                            assert values.length >= 1;

                            res.put(toSimpleObject(keyCls, values[0]),
                                values.length > 1 ? toSimpleObject(valCls, values[1]) : null);
                        }
                    }
                }

                return res;
            }

            if (cls == GridTuple3.class) {
                String v2ClsName = argument(idx + 1);
                String v3ClsName = argument(idx + 2);

                assert v2ClsName != null;
                assert v3ClsName != null;

                Class<?> v1Cls = Class.forName(arg);
                Class<?> v2Cls = Class.forName(v2ClsName);
                Class<?> v3Cls = Class.forName(v3ClsName);

                return new GridTuple3<>(toSimpleObject(v1Cls, (String)argument(idx + 3)), toSimpleObject(v2Cls,
                    (String)argument(idx + 4)), toSimpleObject(v3Cls, (String)argument(idx + 5)));
            }

            return toSimpleObject(cls, arg);
        }

        /**
         * Cast from string representation to target class.
         *
         * @param cls Target class.
         * @return Object constructed from string.
         */
        @Nullable private Object toSimpleObject(Class cls, String val) {
            if (val == null  || val.equals("null"))
                return null;

            if (String.class == cls)
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

            if (Collection.class == cls)
                return Arrays.asList(val.split(";"));

            if (Set.class == cls)
                return new HashSet<>(Arrays.asList(val.split(";")));

            if (Object[].class == cls)
                return val.split(";");

            if (byte[].class == cls) {
                String[] els = val.split(";");

                if (els.length == 0 || (els.length == 1 && els[0].length() == 0))
                    return new byte[0];

                byte[] res = new byte[els.length];

                for (int i = 0; i < els.length; i ++)
                    res[i] =  Byte.valueOf(els[i]);

                return res;
            }

            return val;
        }

        /**
         * Check if class is not a complex bean.
         *
         * @param cls Target class.
         * @return {@code True} if class is primitive or build-in java type or IgniteUuid.
         */
        private static boolean isBuildInObject(Class cls) {
            return cls.isPrimitive() || cls.getName().startsWith("java.") ||
                IgniteUuid.class == cls || IgniteBiTuple.class == cls || GridTuple3.class == cls;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Object execute() throws IgniteException {
            if (fut != null)
                return fut.get();

            String nidsArg = argument(0);
            String taskName = argument(1);

            Object jobArgs = null;

            if (argsCnt > 2) {
                String argClsName = argument(2);

                assert argClsName != null;

                try {
                    Class<?> argCls = Class.forName(argClsName);

                    if (argCls == Void.class)
                        jobArgs = null;
                    else if (isBuildInObject(argCls))
                        jobArgs = toObject(argCls, 3);
                    else {
                        int beanArgsCnt = argsCnt - 3;

                        for (Constructor ctor : argCls.getDeclaredConstructors()) {
                            Class[] types = ctor.getParameterTypes();

                            if (types.length == beanArgsCnt) {
                                Object[] initargs = new Object[beanArgsCnt];

                                for (int i = 0; i < beanArgsCnt; i++) {
                                    String val = argument(i + 3);

                                    initargs[i] = toSimpleObject(types[i], val);
                                }

                                jobArgs = ctor.newInstance(initargs);

                                break;
                            }
                        }

                        if (jobArgs == null)
                            throw new IgniteException("Failed to execute task [task name=" + taskName + "]");
                    }
                }
                catch (Exception e) {
                    throw new IgniteException("Failed to execute task [task name=" + taskName + "]", e);
                }
            }

            final Collection<UUID> nids;

            if (nidsArg == null || nidsArg.equals("null") || nidsArg.equals("")) {
                Collection<ClusterNode> nodes = ignite.cluster().nodes();

                nids = new ArrayList<>(nodes.size());

                for (ClusterNode node : nodes)
                    nids.add(node.id());
            }
            else {
                String[] items = nidsArg.split(";");

                nids = new ArrayList<>(items.length);

                for (String item : items) {
                    try {
                        nids.add(UUID.fromString(item));
                    } catch (IllegalArgumentException ignore) {
                        // No-op.
                    }
                }
            }

            IgniteCompute comp = ignite.compute(ignite.cluster().forNodeIds(nids)).withAsync();
            
            comp.execute(taskName, new VisorTaskArgument<>(nids, jobArgs, false));

            fut = comp.future();

            fut.listen(new CI1<IgniteFuture<Object>>() {
                @Override public void apply(IgniteFuture<Object> f) {
                    jobCtx.callcc();
                }
            });

            return jobCtx.holdcc();
        }
    }
}
