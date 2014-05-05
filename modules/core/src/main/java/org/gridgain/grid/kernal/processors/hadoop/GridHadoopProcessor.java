/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * Hadoop processor.
 */
public abstract class GridHadoopProcessor extends GridProcessorAdapter {
    /** Implementation class name. */
    private static final String CLS_NAME = "org.gridgain.grid.kernal.processors.hadoop.GridHadoopOpProcessor";

    /**
     * @param ctx Kernal context.
     */
    protected GridHadoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Get Hadoop processor instance.
     *
     * @param ctx Kernal context.
     * @param nop Nop flag.
     * @return Created processor.
     * @throws GridException If failed.
     */
    public static GridHadoopProcessor instance(GridKernalContext ctx, boolean nop) throws GridException {
        if (nop)
            return new GridHadoopNopProcessor(ctx);
        else {
            try {
                Class<?> cls = Class.forName(CLS_NAME);

                Constructor<?> ctor = cls.getConstructor(GridKernalContext.class);

                return (GridHadoopProcessor)ctor.newInstance(ctx);
            }
            catch (ClassNotFoundException e) {
                throw new GridException("Failed to instantiate Hadoop processor because it's class is not found " +
                    "(is it in classpath?): " + CLS_NAME, e);
            }
            catch (NoSuchMethodException e) {
                throw new GridException("Failed to instantiate Hadoop processor because it's class doesn't have " +
                    "required constructor (is class version correct?): " + CLS_NAME, e);
            }
            catch (ReflectiveOperationException e) {
                throw new GridException("Failed to instantiate Hadoop processor: " + CLS_NAME, e);
            }
        }
    }



    /**
     * @param cnt Number of IDs to generate.
     * @return Collection of generated IDs.
     */
    public abstract Collection<GridHadoopJobId> getNextJobIds(int cnt);

    /**
     * Submits job to job tracker.
     *
     * @param jobId Job ID to submit.
     * @param jobInfo Job info to submit.
     * @return Execution future.
     */
    public abstract GridFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo);

    /**
     * Gets hadoop job execution status.
     *
     * @param jobId Job ID to get status for.
     * @return Job execution status.
     */
    public abstract GridHadoopJobStatus status(GridHadoopJobId jobId) throws GridException;
}
