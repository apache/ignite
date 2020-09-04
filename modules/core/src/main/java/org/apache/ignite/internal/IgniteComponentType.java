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

package org.apache.ignite.internal;

import java.lang.reflect.Constructor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Component type.
 */
public enum IgniteComponentType {
    /** @deprecated Component was removed. Enum can't be removed because enum ordinal is important. */
    @Deprecated
    IGFS(
        "org.apache.ignite.internal.processors.igfs.IgfsNoopProcessor",
        "org.apache.ignite.internal.processors.igfs.IgfsProcessor",
        "ignite-hadoop"
    ),

    /** @deprecated Component was removed. Enum can't be removed because enum ordinal is important. */
    @Deprecated
    HADOOP(
        "org.apache.ignite.internal.processors.hadoop.HadoopNoopProcessor",
        "org.apache.ignite.internal.processors.hadoop.HadoopProcessor",
        "ignite-hadoop"
    ),

    /** @deprecated Component was removed. Enum can't be removed because enum ordinal is important. */
    @Deprecated
    HADOOP_HELPER(
        "org.apache.ignite.internal.processors.hadoop.HadoopNoopHelper",
        "org.apache.ignite.internal.processors.hadoop.HadoopHelperImpl",
        "ignite-hadoop"
    ),

    /** @deprecated Component was removed. Enum can't be removed because enum ordinal is important. */
    @Deprecated
    IGFS_HELPER(
        "org.apache.ignite.internal.processors.igfs.IgfsNoopHelper",
        "org.apache.ignite.internal.processors.igfs.IgfsHelperImpl",
        "ignite-hadoop"
    ),

    /** Spring XML parsing. */
    SPRING(
        null,
        "org.apache.ignite.internal.util.spring.IgniteSpringHelperImpl",
        "ignite-spring"
    ),

    /** Indexing. */
    INDEXING(
        null,
        "org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing",
        "ignite-indexing",
        "org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory"
    ),

    /** Nodes starting using SSH. */
    SSH(
        null,
        "org.apache.ignite.internal.util.nodestart.IgniteSshHelperImpl",
        "ignite-ssh"
    ),

    /** Integration of cache transactions with JTA. */
    JTA(
        "org.apache.ignite.internal.processors.cache.jta.CacheNoopJtaManager",
        "org.apache.ignite.internal.processors.cache.jta.CacheJtaManager",
        "ignite-jta"
    ),

    /** Cron-based scheduling, see {@link org.apache.ignite.IgniteScheduler}. */
    SCHEDULE(
        "org.apache.ignite.internal.processors.schedule.IgniteNoopScheduleProcessor",
        "org.apache.ignite.internal.processors.schedule.IgniteScheduleProcessor",
        "ignite-schedule"
    ),

    /** */
    COMPRESSION(
        CompressionProcessor.class.getName(),
        "org.apache.ignite.internal.processors.compress.CompressionProcessorImpl",
        "ignite-compress"
    ),

    /** OpenCensus tracing implementation. */
    TRACING(
        null,
        "org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi",
        "ignite-opencensus"
    );

    /** No-op class name. */
    private final String noOpClsName;

    /** Class name. */
    private final String clsName;

    /** Module name. */
    private final String module;

    /** Optional message factory for component. */
    private final String msgFactoryCls;

    /**
     * Constructor.
     *
     * @param noOpClsName Class name for no-op implementation.
     * @param clsName Class name.
     * @param module Module name.
     */
    IgniteComponentType(String noOpClsName, String clsName, String module) {
        this(noOpClsName, clsName, module, null);
    }

    /**
     * Constructor.
     *
     * @param noOpClsName Class name for no-op implementation.
     * @param clsName Class name.
     * @param module Module name.
     * @param msgFactoryCls {@link MessageFactory} class for the component.
     */
    IgniteComponentType(String noOpClsName, String clsName, String module, String msgFactoryCls) {
        this.noOpClsName = noOpClsName;
        this.clsName = clsName;
        this.module = module;
        this.msgFactoryCls = msgFactoryCls;
    }

    /**
     * @return Component class name.
     */
    public String className() {
        return clsName;
    }

    /**
     * @return Component module name.
     */
    public String module() {
        return module;
    }

    /**
     * Check whether real component class is in classpath.
     *
     * @return {@code True} if in classpath.
     */
    public boolean inClassPath() {
        return IgniteUtils.inClassPath(clsName);
    }

    /**
     * Creates component.
     *
     * @param ctx Kernal context.
     * @param noOp No-op flag.
     * @return Created component.
     * @throws IgniteCheckedException If failed.
     */
    public <T> T create(GridKernalContext ctx, boolean noOp) throws IgniteCheckedException {
        return create0(ctx, noOp ? noOpClsName : clsName);
    }

    /**
     * Creates component.
     *
     * @param ctx Kernal context.
     * @param mandatory If the component is mandatory.
     * @return Created component.
     * @throws IgniteCheckedException If failed.
     */
    public <T> T createIfInClassPath(GridKernalContext ctx, boolean mandatory)
        throws IgniteCheckedException {
        String cls = clsName;

        try {
            Class.forName(cls);
        }
        catch (ClassNotFoundException e) {
            if (mandatory)
                throw componentException(e);

            cls = noOpClsName;
        }

        return create0(ctx, cls);
    }

    /**
     * Creates component.
     *
     * @param noOp No-op flag.
     * @return Created component.
     * @throws IgniteCheckedException If failed.
     */
    public <T> T create(boolean noOp) throws IgniteCheckedException {
        return create0(null, noOp ? noOpClsName : clsName);
    }

    /**
     * First tries to find main component class, if it is not found creates no-op implementation.
     *
     * @param ctx Kernal context.
     * @return Created component or no-op implementation.
     * @throws IgniteCheckedException If failed.
     */
    public <T> T createOptional(GridKernalContext ctx) throws IgniteCheckedException {
        return createOptional0(ctx);
    }

    /**
     * First tries to find main component class, if it is not found creates no-op implementation.
     *
     * @return Created component or no-op implementation.
     * @throws IgniteCheckedException If failed.
     */
    public <T> T createOptional() throws IgniteCheckedException {
        return createOptional0(null);
    }

    /**
     * First tries to find main component class, if it is not found creates no-op implementation.
     *
     * @param ctx Kernal context.
     * @return Created component or no-op implementation.
     * @throws IgniteCheckedException If failed.
     */
    private <T> T createOptional0(@Nullable GridKernalContext ctx) throws IgniteCheckedException {
        Class<?> cls;

        try {
            cls = Class.forName(clsName);
        }
        catch (ClassNotFoundException ignored) {
            try {
                cls = Class.forName(noOpClsName);
            }
            catch (ClassNotFoundException e) {
                throw new IgniteCheckedException("Failed to find both real component class and no-op class.", e);
            }
        }

        try {
            if (ctx == null) {
                Constructor<?> ctor = cls.getConstructor();

                return (T)ctor.newInstance();
            }
            else {
                Constructor<?> ctor = cls.getConstructor(GridKernalContext.class);

                return (T)ctor.newInstance(ctx);
            }
        }
        catch (Exception e) {
            throw componentException(e);
        }
    }

    /**
     * Creates component instance.
     *
     * @param ctx Kernal context.
     * @param clsName Component class name.
     * @return Component instance.
     * @throws IgniteCheckedException If failed.
     */
    private <T> T create0(@Nullable GridKernalContext ctx, String clsName) throws IgniteCheckedException {
        try {
            Class<?> cls = Class.forName(clsName);

            if (ctx == null) {
                Constructor<?> ctor = cls.getConstructor();

                return (T)ctor.newInstance();
            }
            else {
                Constructor<?> ctor = cls.getConstructor(GridKernalContext.class);

                return (T)ctor.newInstance(ctx);
            }
        }
        catch (Throwable e) {
            throw componentException(e);
        }
    }

    /**
     * Creates message factory for the component.
     *
     * @return Message factory or {@code null} if none or the component is not in classpath.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public MessageFactory messageFactory() throws IgniteCheckedException {
        Class<?> cls;

        if (msgFactoryCls == null || null == (cls = U.classForName(msgFactoryCls, null)))
            return null;

        return (MessageFactory)U.newInstance(cls);
    }

    /**
     * @param err Creation error.
     * @return Component creation exception.
     */
    private IgniteCheckedException componentException(Throwable err) {
        return new IgniteCheckedException("Failed to create Ignite component (consider adding " + module +
            " module to classpath) [component=" + this + ", cls=" + clsName + ']', err);
    }
}
