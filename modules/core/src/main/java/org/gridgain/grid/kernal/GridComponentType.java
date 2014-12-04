/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;

/**
 * Component type.
 */
public enum GridComponentType {
    /** GGFS. */
    GGFS(
        "org.gridgain.grid.kernal.processors.ggfs.GridNoopGgfsProcessor",
        "org.gridgain.grid.kernal.processors.ggfs.GridGgfsProcessor",
        "gridgain-hadoop"
    ),

    /** Hadoop. */
    HADOOP(
        "org.gridgain.grid.kernal.processors.hadoop.GridHadoopNoopProcessor",
        "org.gridgain.grid.kernal.processors.hadoop.GridHadoopProcessor",
        "gridgain-hadoop"
    ),

    /** GGFS helper component. */
    GGFS_HELPER(
        "org.gridgain.grid.kernal.processors.ggfs.GridNoopGgfsHelper",
        "org.gridgain.grid.kernal.processors.ggfs.GridGgfsHelperImpl",
        "gridgain-hadoop"
    ),

    /** Spring XML parsing. */
    SPRING(
        null,
        "org.gridgain.grid.kernal.processors.spring.GridSpringProcessorImpl",
        "gridgain-spring"
    ),

    /** H2 indexing SPI. */
    H2_INDEXING(
        "org.gridgain.grid.spi.indexing.GridNoopIndexingSpi",
        "org.gridgain.grid.spi.indexing.h2.GridH2IndexingSpi",
        "gridgain-indexing"
    ),

    /** Nodes starting using SSH. */
    SSH(
        null,
        "org.gridgain.grid.util.nodestart.GridSshProcessorImpl",
        "gridgain-ssh"
    ),

    /** Email sending. */
    EMAIL(
        "org.gridgain.grid.kernal.processors.email.GridNoopEmailProcessor",
        "org.gridgain.grid.kernal.processors.email.GridEmailProcessor",
        "gridgain-email"
    ),

    /** Integration of cache transactions with JTA. */
    JTA(
        "org.gridgain.grid.kernal.processors.cache.jta.GridCacheNoopJtaManager",
        "org.gridgain.grid.kernal.processors.cache.jta.GridCacheJtaManager",
        "gridgain-jta"
    ),

    /** Cron-based scheduling, see {@link org.apache.ignite.IgniteScheduler}. */
    SCHEDULE(
        "org.gridgain.grid.kernal.processors.schedule.GridNoopScheduleProcessor",
        "org.gridgain.grid.kernal.processors.schedule.GridScheduleProcessor",
        "gridgain-schedule"
    );

    /** No-op class name. */
    private final String noOpClsName;

    /** Class name. */
    private final String clsName;

    /** Module name. */
    private final String module;

    /**
     * Constructor.
     *
     * @param noOpClsName Class name for no-op implementation.
     * @param clsName Class name.
     * @param module Module name.
     */
    GridComponentType(String noOpClsName, String clsName, String module) {
        this.noOpClsName = noOpClsName;
        this.clsName = clsName;
        this.module = module;
    }

    /**
     * @return Component class name.
     */
    public String className() {
        return clsName;
    }

    /**
     * Check whether real component class is in classpath.
     *
     * @return {@code True} if in classpath.
     */
    public boolean inClassPath() {
        try {
            Class.forName(clsName);

            return true;
        }
        catch (ClassNotFoundException ignore) {
            return false;
        }
    }

    /**
     * Creates component.
     *
     * @param ctx Kernal context.
     * @param noOp No-op flag.
     * @return Created component.
     * @throws GridException If failed.
     */
    public <T extends GridComponent> T create(GridKernalContext ctx, boolean noOp) throws GridException {
        return create0(ctx, noOp ? noOpClsName : clsName);
    }

    /**
     * Creates component.
     *
     * @param ctx Kernal context.
     * @param mandatory If the component is mandatory.
     * @return Created component.
     * @throws GridException If failed.
     */
    public <T extends GridComponent> T createIfInClassPath(GridKernalContext ctx, boolean mandatory)
        throws GridException {
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
     * @throws GridException If failed.
     */
    public <T> T create(boolean noOp) throws GridException {
        return create0(null, noOp ? noOpClsName : clsName);
    }

    /**
     * First tries to find main component class, if it is not found creates no-op implementation.
     *
     * @param ctx Kernal context.
     * @return Created component or no-op implementation.
     * @throws GridException If failed.
     */
    public <T> T createOptional(GridKernalContext ctx) throws GridException {
        return createOptional0(ctx);
    }

    /**
     * First tries to find main component class, if it is not found creates no-op implementation.
     *
     * @return Created component or no-op implementation.
     * @throws GridException If failed.
     */
    public <T> T createOptional() throws GridException {
        return createOptional0(null);
    }

    /**
     * First tries to find main component class, if it is not found creates no-op implementation.
     *
     * @param ctx Kernal context.
     * @return Created component or no-op implementation.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private <T> T createOptional0(@Nullable GridKernalContext ctx) throws GridException {
        Class<?> cls;

        try {
            cls = Class.forName(clsName);
        }
        catch (ClassNotFoundException ignored) {
            try {
                cls = Class.forName(noOpClsName);
            }
            catch (ClassNotFoundException e) {
                throw new GridException("Failed to find both real component class and no-op class.", e);
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
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private <T> T create0(@Nullable GridKernalContext ctx, String clsName) throws GridException {
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
        catch (Exception e) {
            throw componentException(e);
        }
    }

    /**
     * @param err Creation error.
     * @return Component creation exception.
     */
    private GridException componentException(Exception err) {
        return new GridException("Failed to create GridGain component (consider adding " + module +
            " module to classpath) [component=" + this + ", cls=" + clsName + ']', err);
    }
}
