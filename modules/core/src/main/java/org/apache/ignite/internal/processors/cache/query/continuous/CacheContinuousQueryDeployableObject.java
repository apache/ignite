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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteDeploymentCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Deployable object.
 */
class CacheContinuousQueryDeployableObject implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Serialized object. */
    @GridToStringExclude
    private byte[] bytes;

    /** Deployment class name. */
    private String clsName;

    /** Deployment info. */
    private GridDeploymentInfo depInfo;

    /**
     * Required by {@link Externalizable}.
     */
    public CacheContinuousQueryDeployableObject() {
        // No-op.
    }

    /**
     * @param obj Object.
     * @param ctx Kernal context.
     * @throws IgniteCheckedException In case of error.
     */
    protected CacheContinuousQueryDeployableObject(Object obj, GridKernalContext ctx) throws IgniteCheckedException {
        assert obj != null;
        assert ctx != null;

        Class cls = U.detectClass(obj);

        clsName = cls.getName();

        GridDeployment dep = ctx.deploy().deploy(cls, U.detectClassLoader(cls));

        if (dep == null)
            throw new IgniteDeploymentCheckedException("Failed to deploy object: " + obj);

        depInfo = new GridDeploymentInfoBean(dep);

        bytes = U.marshal(ctx, obj);
    }

    /**
     * @param nodeId Node ID.
     * @param ctx Kernal context.
     * @return Deserialized object.
     * @throws IgniteCheckedException In case of error.
     */
    <T> T unmarshal(UUID nodeId, GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        GridDeployment dep = ctx.deploy().getGlobalDeployment(depInfo.deployMode(), clsName, clsName,
            depInfo.userVersion(), nodeId, depInfo.classLoaderId(), depInfo.participants(), null);

        if (dep == null)
            throw new IgniteDeploymentCheckedException("Failed to obtain deployment for class: " + clsName);

        return U.unmarshal(ctx, bytes, U.resolveClassLoader(dep.classLoader(), ctx.config()));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeByteArray(out, bytes);
        U.writeString(out, clsName);
        out.writeObject(depInfo);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bytes = U.readByteArray(in);
        clsName = U.readString(in);
        depInfo = (GridDeploymentInfo)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryDeployableObject.class, this);
    }
}
