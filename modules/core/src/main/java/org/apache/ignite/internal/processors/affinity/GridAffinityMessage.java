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
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Object wrapper containing serialized byte array of original object and deployment information.
 */
class GridAffinityMessage implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private byte[] src;

    /** */
    private IgniteUuid clsLdrId;

    /** */
    private DeploymentMode depMode;

    /** */
    private String srcClsName;

    /** */
    private String userVer;

    /** Node class loader participants. */
    @GridToStringInclude
    private Map<UUID, IgniteUuid> ldrParties;

    /**
     * @param src Source object.
     * @param srcClsName Source object class name.
     * @param clsLdrId Class loader ID.
     * @param depMode Deployment mode.
     * @param userVer User version.
     * @param ldrParties Node loader participant map.
     */
    GridAffinityMessage(
        byte[] src,
        String srcClsName,
        IgniteUuid clsLdrId,
        DeploymentMode depMode,
        String userVer,
        Map<UUID, IgniteUuid> ldrParties) {
        this.src = src;
        this.srcClsName = srcClsName;
        this.depMode = depMode;
        this.clsLdrId = clsLdrId;
        this.userVer = userVer;
        this.ldrParties = ldrParties;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridAffinityMessage() {
        // No-op.
    }

    /**
     * @return Source object.
     */
    public byte[] source() {
        return src;
    }

    /**
     * @return the Class loader ID.
     */
    public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * @return Deployment mode.
     */
    public DeploymentMode deploymentMode() {
        return depMode;
    }

    /**
     * @return Source message class name.
     */
    public String sourceClassName() {
        return srcClsName;
    }

    /**
     * @return User version.
     */
    public String userVersion() {
        return userVer;
    }

    /**
     * @return Node class loader participant map.
     */
    public Map<UUID, IgniteUuid> loaderParticipants() {
        return ldrParties != null ? Collections.unmodifiableMap(ldrParties) : null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeByteArray(out, src);

        out.writeInt(depMode.ordinal());

        U.writeGridUuid(out, clsLdrId);
        U.writeString(out, srcClsName);
        U.writeString(out, userVer);
        U.writeMap(out, ldrParties);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        src = U.readByteArray(in);

        depMode = DeploymentMode.fromOrdinal(in.readInt());

        clsLdrId = U.readGridUuid(in);
        srcClsName = U.readString(in);
        userVer = U.readString(in);
        ldrParties = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAffinityMessage.class, this);
    }
}