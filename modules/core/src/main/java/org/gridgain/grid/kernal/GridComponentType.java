// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.scheduler.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;

/**
 * Component type.
 */
public enum GridComponentType {
    /** GGFS component. */
    COMP_GGFS(
        "org.gridgain.grid.kernal.processors.ggfs.GridGgfsNoopProcessor",
        "org.gridgain.grid.kernal.processors.ggfs.GridGgfsProcessor"
    ),
    /** Spring XML parsing. */
    SPRING(
        null,
        "org.gridgain.grid.kernal.processors.spring.GridSpringProcessorImpl"
    ),
    /** H2 indexing SPI. */
    H2_INDEXING(
        "org.gridgain.grid.spi.indexing.GridIndexingNoopSpi",
        "org.gridgain.grid.spi.indexing.h2.GridH2IndexingSpi"
    ),
    /** Nodes starting using SSH. */
    SSH(
        null,
        "org.gridgain.grid.util.nodestart.GridSshProcessorImpl"
    ),
    /** REST access. */
    REST(
        "org.gridgain.grid.kernal.processors.rest.GridRestNoopProcessor",
        "org.gridgain.grid.kernal.processors.rest.GridRestProcessor"
    ),
    /** Email sending. */
    EMAIL(
        "org.gridgain.grid.kernal.processors.email.GridEmailNoopProcessor",
        "org.gridgain.grid.kernal.processors.email.GridEmailProcessor"
    ),
    /** Integration of cache transactions with JTA. */
    JTA(
        "org.gridgain.grid.kernal.processors.cache.jta.GridCacheJtaNoopManager",
        "org.gridgain.grid.kernal.processors.cache.jta.GridCacheJtaManager"),
    /** Cron-based scheduling, see {@link GridScheduler}. */
    SCHEDULE(
        "org.gridgain.grid.kernal.processors.schedule.GridScheduleNoopProcessor",
        "org.gridgain.grid.kernal.processors.schedule.GridScheduleProcessor");

    /** No-op class name. */
    private final String noOpClsName;

    /** Class name. */
    private final String clsName;

    /**
     * Constructor.
     *
     * @param noOpClsName Class name for no-op implementation.
     * @param clsName Class name.
     */
    GridComponentType(String noOpClsName, String clsName) {
        this.noOpClsName = noOpClsName;
        this.clsName = clsName;
    }

    /**
     * @return Component class name.
     */
    public String className() {
        return clsName;
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
            throw new GridException("Failed to create component.", e);
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
        catch (ClassNotFoundException e) {
            throw new GridException("Failed to create GridGain component because it's class is not found " +
                "(is it in classpath?) [component=" + this + ", class=" + clsName + ']', e);
        }
        catch (NoSuchMethodException e) {
            throw new GridException("Failed to create GridGain component because it's class doesn't have " +
                "required constructor (is class version correct?) [component=" + this + ", class=" +
                clsName + ']', e);
        }
        catch (ReflectiveOperationException e) {
            throw new GridException("Failed to instantiate GridGain component [component=" + this + ", class=" +
                clsName + ']', e);
        }
    }
}
