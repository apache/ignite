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

package org.apache.ignite.internal.management;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker.AttributeWithValueVisitor;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.DATE;
import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.NUMBER;
import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.STRING;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.toSqlName;

/** Reperesents visor task for obtaining system view content. */
@GridInternal
@GridVisorManagementTask
public class SystemViewTask extends VisorMultiNodeTask<SystemViewCommandArg, SystemViewTaskResult,
    SystemViewTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<SystemViewCommandArg, SystemViewTaskResult> job(SystemViewCommandArg arg) {
        return new VisorSystemViewJob(arg, false);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable SystemViewTaskResult reduce0(List<ComputeJobResult> results)
        throws IgniteException {
        SystemViewTaskResult res = null;

        Map<UUID, List<List<?>>> merged = new TreeMap<>();

        for (ComputeJobResult r : results) {
            if (r.getException() != null)
                throw new IgniteException("Failed to execute job [nodeId=" + r.getNode().id() + ']', r.getException());

            res = r.getData();

            if (res == null)
                return null;

            merged.putAll(res.rows());
        }

        return new SystemViewTaskResult(res.attributes(), res.types(), merged);
    }

    /** */
    private static class VisorSystemViewJob extends VisorJob<SystemViewCommandArg, SystemViewTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSystemViewJob(@Nullable SystemViewCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected SystemViewTaskResult run(@Nullable SystemViewCommandArg arg) throws IgniteException {
            if (arg == null)
                return null;

            SystemView<?> sysView = systemView(arg.systemViewName());

            if (sysView == null)
                return null;

            List<String> attrNames = new ArrayList<>();
            List<SimpleType> attrTypes = new ArrayList<>();

            sysView.walker().visitAll(new SystemViewRowAttributeWalker.AttributeVisitor() {
                @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                    attrNames.add(name);

                    Class<?> wrapperCls = U.box(clazz);

                    if (Number.class.isAssignableFrom(wrapperCls))
                        attrTypes.add(NUMBER);
                    else if (Date.class.isAssignableFrom(wrapperCls))
                        attrTypes.add(DATE);
                    else
                        attrTypes.add(STRING);
                }
            });

            List<List<?>> rows = new ArrayList<>();

            for (Object row : sysView) {
                List<Serializable> attrVals = new ArrayList<>();

                ((SystemView<Object>)sysView).walker().visitAll(row, new AttributeWithValueVisitor() {
                    @Override public <T> void accept(int idx, String name, Class<T> clazz, @Nullable T val) {
                        if (clazz.isEnum())
                            attrVals.add(val == null ? null : ((Enum<?>)val).name());
                        else if (Class.class.isAssignableFrom(clazz))
                            attrVals.add(val == null ? null : ((Class<?>)val).getName());
                        else if (
                            Date.class.isAssignableFrom(clazz) ||
                            UUID.class.isAssignableFrom(clazz) ||
                            IgniteUuid.class.isAssignableFrom(clazz)
                        )
                            attrVals.add((Serializable)val);
                        else
                            attrVals.add(String.valueOf(val));
                    }

                    @Override public void acceptBoolean(int idx, String name, boolean val) {
                        attrVals.add(val);
                    }

                    @Override public void acceptChar(int idx, String name, char val) {
                        attrVals.add(val);
                    }

                    @Override public void acceptByte(int idx, String name, byte val) {
                        attrVals.add(val);
                    }

                    @Override public void acceptShort(int idx, String name, short val) {
                        attrVals.add(val);
                    }

                    @Override public void acceptInt(int idx, String name, int val) {
                        attrVals.add(val);
                    }

                    @Override public void acceptLong(int idx, String name, long val) {
                        attrVals.add(val);
                    }

                    @Override public void acceptFloat(int idx, String name, float val) {
                        attrVals.add(val);
                    }

                    @Override public void acceptDouble(int idx, String name, double val) {
                        attrVals.add(val);
                    }
                });

                rows.add(attrVals);
            }

            return new SystemViewTaskResult(attrNames, attrTypes, singletonMap(ignite.localNode().id(), rows));
        }

        /**
         * Gets system view with specified name.
         *
         * @param name Name of system view. Both "SQL" and "Java" styles of name are suppurted.
         * @return System view.
         */
        private SystemView<?> systemView(String name) {
            GridSystemViewManager sysViewMgr = ignite.context().systemView();

            SystemView<?> res = sysViewMgr.view(name);

            if (res == null) { // In this case we assume that the requested system view name is in SQL format.
                for (SystemView<?> sysView : sysViewMgr) {
                    if (toSqlName(sysView.name()).toLowerCase().equals(name.toLowerCase())) {
                        res = sysView;

                        break;
                    }
                }
            }

            return res;
        }
    }

    /**
     * Represents lightweight type descriptors.
     */
    public enum SimpleType {
        /** Date. */
        DATE,

        /** Number. */
        NUMBER,

        /** String. */
        STRING
    }
}
