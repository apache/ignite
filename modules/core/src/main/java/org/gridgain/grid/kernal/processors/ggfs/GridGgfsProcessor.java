// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.util.ipc.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.util.*;

public abstract class GridGgfsProcessor extends GridProcessorAdapter {
    /** Real processor class name. */
    private static final String CLS_NAME = "org.gridgain.grid.kernal.processors.ggfs.GridGgfsOpProcessor";

    /**
     * Get GGFS processor instance.
     *
     * @param ctx Kernal context.
     * @param nop Nop flag.
     * @return Created processor.
     * @throws GridException If failed.
     */
    public static GridGgfsProcessor instance(GridKernalContext ctx, boolean nop) throws GridException {
        if (nop)
            return new GridGgfsNopProcessor(ctx);
        else {
            try {
                Class<?> cls = Class.forName(CLS_NAME);

                Constructor<?> ctor = cls.getConstructor(GridKernalContext.class);

                return (GridGgfsProcessor)ctor.newInstance(ctx);
            }
            catch (ClassNotFoundException e) {
                throw new GridException("Failed to instantiate GGFS processor because it's class is not found " +
                    "(is it in classpath?): " + CLS_NAME, e);
            }
            catch (NoSuchMethodException e) {
                throw new GridException("Failed to instantiate GGFS processor because it's class doesn't have " +
                    "required constructor (is class version correct?): " + CLS_NAME, e);
            }
            catch (ReflectiveOperationException e) {
                throw new GridException("Failed to instantiate GGFS processor: " + CLS_NAME, e);
            }
        }
    }

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    protected GridGgfsProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Gets all GGFS instances.
     *
     * @return Collection of GGFS instances.
     */
    public abstract Collection<GridGgfs> ggfss();

    /**
     * Gets GGFS instance.
     *
     * @param name (Nullable) GGFS name.
     * @return GGFS instance.
     */
    @Nullable public abstract GridGgfs ggfs(@Nullable String name);

    /**
     * Gets server endpoints for particular GGFS.
     *
     * @param name GGFS name.
     * @return Collection of endpoints or {@code null} in case GGFS is not defined.
     */
    public abstract Collection<GridIpcServerEndpoint> endpoints(@Nullable String name);

    /**
     * Create compute job for the given GGFs job.
     *
     * @param job GGFS job.
     * @param ggfsName GGFS name.
     * @param path Path.
     * @param start Start position.
     * @param length Length.
     * @param recRslv Record resolver.
     * @return Compute job.
     */
    @Nullable public abstract GridComputeJob createJob(GridGgfsJob job, @Nullable String ggfsName, GridGgfsPath path,
        long start, long length, GridGgfsRecordResolver recRslv);

    /**
     * Check whether object is os type {@code GridGgfsBlockKey}
     *
     * @param key Key.
     * @return {@code True} if GGFS block key.
     */
    public abstract boolean isGgfsBlockKey(Object key);

    /**
     * Pre-process cache configuration.
     *
     * @param cfg Cache configuration.
     */
    public abstract void preProcessCacheConfiguration(GridCacheConfiguration cfg);

    /**
     * Validate cache configuration for GGFS.
     *
     * @param cfg Cache configuration.
     * @throws GridException If validation failed.
     */
    public abstract void validateCacheConfiguration(GridCacheConfiguration cfg) throws GridException;
}
