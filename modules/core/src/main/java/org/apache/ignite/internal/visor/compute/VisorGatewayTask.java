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
import org.apache.ignite.internal.visor.VisorCoordinatorNodeTask;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task to run Visor tasks through http REST.
 */
@GridInternal
public class VisorGatewayTask implements ComputeTask<Object[], Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int JOB_ARG_IDX = 3;

    /** Array with additional length in arguments for specific nested types */
    private static final Map<Class, Integer> TYPE_ARG_LENGTH = new HashMap<>(4);

    static {
        TYPE_ARG_LENGTH.put(Collection.class, 2);
        TYPE_ARG_LENGTH.put(Set.class, 2);
        TYPE_ARG_LENGTH.put(List.class, 2);
        TYPE_ARG_LENGTH.put(Map.class, 3);
        TYPE_ARG_LENGTH.put(IgniteBiTuple.class, 4);
        TYPE_ARG_LENGTH.put(GridTuple3.class, 6);
    }

    /** Auto-injected grid instance. */
    @IgniteInstanceResource
    protected transient IgniteEx ignite;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
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

        /** */
        private static final byte[] ZERO_BYTES = new byte[0];

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
         * Construct job argument.
         *
         * @param cls Class.
         * @param startIdx Index of first value argument.
         */
        @Nullable private Object toJobArgument(Class cls, int startIdx) throws ClassNotFoundException {
            String arg = argument(startIdx);

            boolean isList = cls == Collection.class || cls == List.class;

            if (isList || cls == Set.class) {
                String items = argument(startIdx + 1);

                if (items == null || "null".equals(items))
                    return null;

                Class<?> itemsCls = Class.forName(arg);

                Collection<Object> res = isList ? new ArrayList<>() : new HashSet<>();

                for (String item : items.split(";"))
                    res.add(toObject(itemsCls, item));

                return res;
            }

            if (cls == IgniteBiTuple.class) {
                Class<?> keyCls = Class.forName(arg);

                String valClsName = argument(startIdx + 1);

                assert valClsName != null;

                Class<?> valCls = Class.forName(valClsName);

                return new IgniteBiTuple<>(
                    toObject(keyCls, argument(startIdx + 2)),
                    toObject(valCls, argument(startIdx + 3)));
            }

            if (cls == Map.class) {
                Class<?> keyCls = Class.forName(arg);

                String valClsName = argument(startIdx + 1);

                assert valClsName != null;

                Class<?> valCls = Class.forName(valClsName);

                Map<Object, Object> res = new HashMap<>();

                String entries = argument(startIdx + 2);

                if (entries != null) {
                    for (String entry : entries.split(";")) {
                        if (!entry.isEmpty()) {
                            String[] values = entry.split("=");

                            assert values.length >= 1;

                            res.put(toObject(keyCls, values[0]),
                                values.length > 1 ? toObject(valCls, values[1]) : null);
                        }
                    }
                }

                return res;
            }

            if (cls == GridTuple3.class) {
                String v2ClsName = argument(startIdx + 1);
                String v3ClsName = argument(startIdx + 2);

                assert v2ClsName != null;
                assert v3ClsName != null;

                Class<?> v1Cls = Class.forName(arg);
                Class<?> v2Cls = Class.forName(v2ClsName);
                Class<?> v3Cls = Class.forName(v3ClsName);

                return new GridTuple3<>(
                    toObject(v1Cls, argument(startIdx + 3)),
                    toObject(v2Cls, argument(startIdx + 4)),
                    toObject(v3Cls, argument(startIdx + 5)));
            }

            return toObject(cls, arg);
        }

        /**
         * Construct from string representation to target class.
         *
         * @param cls Target class.
         * @return Object constructed from string.
         */
        @Nullable private Object toObject(Class cls, String val) {
            if (val == null || "null".equals(val) || "nil".equals(val))
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

            if (Collection.class == cls || List.class == cls)
                return Arrays.asList(val.split(";"));

            if (Set.class == cls)
                return new HashSet<>(Arrays.asList(val.split(";")));

            if (Object[].class == cls)
                return val.split(";");

            if (byte[].class == cls) {
                String[] els = val.split(";");

                if (els.length == 0 || (els.length == 1 && els[0].isEmpty()))
                    return ZERO_BYTES;

                byte[] res = new byte[els.length];

                for (int i = 0; i < els.length; i++)
                    res[i] = Byte.valueOf(els[i]);

                return res;
            }

            if (cls.isEnum())
                return Enum.valueOf(cls, val);

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

        /**
         * Extract Class object from arguments.
         *
         * @param idx Index of argument.
         */
        private Class toClass(int idx) throws ClassNotFoundException {
            Object arg = argument(idx);  // Workaround generics: extract argument as Object to use in String.valueOf().

            return Class.forName(String.valueOf(arg));
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
                        jobArgs = toJobArgument(argCls, JOB_ARG_IDX);
                    else {
                        int beanArgsCnt = argsCnt - JOB_ARG_IDX;

                        for (Constructor ctor : argCls.getDeclaredConstructors()) {
                            Class[] types = ctor.getParameterTypes();

                            int args = types.length;

                            // Length of arguments that required to constructor by influence of nested complex objects.
                            int needArgs = args;

                            for (Class type: types) {
                                // When constructor required specified types increase length of required arguments.
                                if (TYPE_ARG_LENGTH.containsKey(type))
                                    needArgs += TYPE_ARG_LENGTH.get(type);
                            }

                            if (needArgs == beanArgsCnt) {
                                Object[] initArgs = new Object[args];

                                for (int i = 0, ctrIdx = 0; i < beanArgsCnt; i++, ctrIdx++) {
                                    Class type = types[ctrIdx];

                                    // Parse nested complex objects from arguments for specified types.
                                    if (TYPE_ARG_LENGTH.containsKey(type)) {
                                        initArgs[ctrIdx] = toJobArgument(toClass(JOB_ARG_IDX + i), JOB_ARG_IDX + 1 + i);

                                        i += TYPE_ARG_LENGTH.get(type);
                                    }
                                    // In common case convert value to object.
                                    else {
                                        String val = argument(JOB_ARG_IDX + i);

                                        initArgs[ctrIdx] = toObject(type, val);
                                    }
                                }

                                ctor.setAccessible(true);
                                jobArgs = ctor.newInstance(initArgs);

                                break;
                            }
                        }

                        if (jobArgs == null) {
                            Object[] args = new Object[beanArgsCnt];

                            for (int i = 0; i < beanArgsCnt; i++)
                                args[i] = argument(i + JOB_ARG_IDX);

                            throw new IgniteException("Failed to find constructor for task argument " +
                                "[taskName=" + taskName + ", argsCnt=" + args.length +
                                ", args=" + Arrays.toString(args) + "]");
                        }
                    }
                }
                catch (Exception e) {
                    throw new IgniteException("Failed to construct job argument [taskName=" + taskName + "]", e);
                }
            }

            final List<UUID> nids;

            if (F.isEmpty(nidsArg) || "null".equals(nidsArg)) {
                try {
                    Class<?> taskCls = Class.forName(taskName);

                    if (VisorCoordinatorNodeTask.class.isAssignableFrom(taskCls)) {
                        ClusterNode crd = ignite.context().discovery().discoCache().oldestAliveServerNode();

                        nids = Collections.singletonList(crd.id());
                    }
                    else if (VisorOneNodeTask.class.isAssignableFrom(taskCls))
                        nids = Collections.singletonList(ignite.localNode().id());
                    else {
                        Collection<ClusterNode> nodes = ignite.cluster().nodes();

                        nids = new ArrayList<>(nodes.size());

                        for (ClusterNode node : nodes)
                            nids.add(node.id());
                    }
                }
                catch (ClassNotFoundException e) {
                    throw new IgniteException("Failed to find task class:" + taskName, e);
                }
            }
            else {
                String[] items = nidsArg.split(";");

                nids = new ArrayList<>(items.length);

                for (String item : items) {
                    try {
                        nids.add(UUID.fromString(item));
                    } catch (IllegalArgumentException ignore) {
                        ignite.log().warning("Failed to parse node id [taskName=" + taskName + ", nid=" + item + "]");
                    }
                }
            }

            IgniteCompute comp = ignite.compute(ignite.cluster().forNodeIds(nids));

            fut = comp.executeAsync(taskName, new VisorTaskArgument<>(nids, jobArgs, false));

            fut.listen(new CI1<IgniteFuture<Object>>() {
                @Override public void apply(IgniteFuture<Object> f) {
                    jobCtx.callcc();
                }
            });

            return jobCtx.holdcc();
        }
    }
}
