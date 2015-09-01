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

package org.apache.ignite.internal.processors.affinity;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteDeploymentCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Affinity utility methods.
 */
class GridAffinityUtils {
    /**
     * Creates a job that will look up {@link AffinityKeyMapper} and {@link AffinityFunction} on a
     * cache with given name. If they exist, this job will serialize and transfer them together with all deployment
     * information needed to unmarshal objects on remote node. Result is returned as a {@link GridTuple3},
     * where first object is {@link GridAffinityMessage} for {@link AffinityFunction}, second object
     * is {@link GridAffinityMessage} for {@link AffinityKeyMapper} and third object is affinity assignment
     * for given topology version.
     *
     * @param cacheName Cache name.
     * @return Affinity job.
     */
    static Callable<GridTuple3<GridAffinityMessage, GridAffinityMessage, GridAffinityAssignment>> affinityJob(
        String cacheName, AffinityTopologyVersion topVer) {
        return new AffinityJob(cacheName, topVer);
    }

    /**
     * @param ctx  {@code GridKernalContext} instance which provides deployment manager
     * @param o Object for which deployment should be obtained.
     * @return Deployment object for given instance,
     * @throws IgniteCheckedException If node cannot create deployment for given object.
     */
    private static GridAffinityMessage affinityMessage(GridKernalContext ctx, Object o) throws IgniteCheckedException {
        Class cls = o.getClass();

        GridDeployment dep = ctx.deploy().deploy(cls, cls.getClassLoader());

        if (dep == null)
            throw new IgniteDeploymentCheckedException("Failed to deploy affinity object with class: " + cls.getName());

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
     * @throws IgniteCheckedException If node cannot obtain deployment.
     */
    static Object unmarshall(GridKernalContext ctx, UUID sndNodeId, GridAffinityMessage msg)
        throws IgniteCheckedException {
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
            throw new IgniteDeploymentCheckedException("Failed to obtain affinity object (is peer class loading turned on?): " +
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
        @LoggerResource
        private IgniteLogger log;

        /** */
        private String cacheName;

        /** */
        private AffinityTopologyVersion topVer;

        /**
         * @param cacheName Cache name.
         */
        private AffinityJob(@Nullable String cacheName, @NotNull AffinityTopologyVersion topVer) {
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

            IgniteKernal kernal = ((IgniteKernal) ignite);

            GridCacheContext<Object, Object> cctx = kernal.internalCache(cacheName).context();

            assert cctx != null;

            GridKernalContext ctx = kernal.context();

            cctx.affinity().affinityReadyFuture(topVer).get();

            return F.t(
                affinityMessage(ctx, cctx.config().getAffinity()),
                affinityMessage(ctx, cctx.config().getAffinityMapper()),
                new GridAffinityAssignment(topVer, cctx.affinity().assignments(topVer)));
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, cacheName);
            out.writeObject(topVer);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);
            topVer = (AffinityTopologyVersion)in.readObject();
        }
    }
}