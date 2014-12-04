/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.affinity;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Affinity utility methods.
 */
class GridAffinityUtils {
    /**
     * Creates a job that will look up {@link GridCacheAffinityKeyMapper} and {@link GridCacheAffinityFunction} on a
     * cache with given name. If they exist, this job will serialize and transfer them together with all deployment
     * information needed to unmarshal objects on remote node. Result is returned as a {@link GridTuple3},
     * where first object is {@link GridAffinityMessage} for {@link GridCacheAffinityFunction}, second object
     * is {@link GridAffinityMessage} for {@link GridCacheAffinityKeyMapper} and third object is affinity assignment
     * for given topology version.
     *
     * @param cacheName Cache name.
     * @return Affinity job.
     */
    static Callable<GridTuple3<GridAffinityMessage, GridAffinityMessage, GridAffinityAssignment>> affinityJob(
        String cacheName, long topVer) {
        return new AffinityJob(cacheName, topVer);
    }

    /**
     * @param ctx  {@code GridKernalContext} instance which provides deployment manager
     * @param o Object for which deployment should be obtained.
     * @return Deployment object for given instance,
     * @throws GridException If node cannot create deployment for given object.
     */
    private static GridAffinityMessage affinityMessage(GridKernalContext ctx, Object o) throws GridException {
        Class cls = o.getClass();

        GridDeployment dep = ctx.deploy().deploy(cls, cls.getClassLoader());

        if (dep == null)
            throw new GridDeploymentException("Failed to deploy affinity object with class: " + cls.getName());

        return new GridAffinityMessage(
            ctx.config().getMarshaller().marshal(o),
            cls.getName(),
            dep.classLoaderId(),
            dep.deployMode(),
            dep.userVersion(),
            dep.participants());
    }

    /**
     * Unmarshalls transfer object from remote node within a given context.
     *
     * @param ctx Grid kernal context that provides deployment and marshalling services.
     * @param sndNodeId {@link UUID} of the sender node.
     * @param msg Transfer object that contains original serialized object and deployment information.
     * @return Unmarshalled object.
     * @throws GridException If node cannot obtain deployment.
     */
    static Object unmarshall(GridKernalContext ctx, UUID sndNodeId, GridAffinityMessage msg)
        throws GridException {
        GridDeployment dep = ctx.deploy().getGlobalDeployment(
            msg.deploymentMode(),
            msg.sourceClassName(),
            msg.sourceClassName(),
            msg.userVersion(),
            sndNodeId,
            msg.classLoaderId(),
            msg.loaderParticipants(),
            null);

        if (dep == null)
            throw new GridDeploymentException("Failed to obtain affinity object (is peer class loading turned on?): " +
                msg);

        Object src = ctx.config().getMarshaller().unmarshal(msg.source(), dep.classLoader());

        // Resource injection.
        ctx.resource().inject(dep, dep.deployedClass(msg.sourceClassName()), src);

        return src;
    }

    /** Ensure singleton. */
    private GridAffinityUtils() {
        // No-op.
    }

    /**
     *
     */
    @GridInternal
    private static class AffinityJob implements
        Callable<GridTuple3<GridAffinityMessage, GridAffinityMessage, GridAffinityAssignment>>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @GridLoggerResource
        private GridLogger log;

        /** */
        private String cacheName;

        /** */
        private long topVer;

        /**
         * @param cacheName Cache name.
         */
        private AffinityJob(@Nullable String cacheName, long topVer) {
            this.cacheName = cacheName;
            this.topVer = topVer;
        }

        /**
         *
         */
        public AffinityJob() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public GridTuple3<GridAffinityMessage, GridAffinityMessage, GridAffinityAssignment> call()
            throws Exception {
            assert ignite != null;
            assert log != null;

            GridKernal kernal = ((GridKernal) ignite);

            GridCacheContext<Object, Object> cctx = kernal.internalCache(cacheName).context();

            assert cctx != null;

            GridKernalContext ctx = kernal.context();

            return F.t(
                affinityMessage(ctx, cctx.config().getAffinity()),
                affinityMessage(ctx, cctx.config().getAffinityMapper()),
                new GridAffinityAssignment(topVer, cctx.affinity().assignments(topVer)));
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, cacheName);
            out.writeLong(topVer);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);
            topVer = in.readLong();
        }
    }
}
