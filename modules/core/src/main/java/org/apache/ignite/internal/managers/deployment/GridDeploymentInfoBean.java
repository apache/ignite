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

package org.apache.ignite.internal.managers.deployment;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Deployment info bean.
 */
public class GridDeploymentInfoBean implements Message, GridDeploymentInfo, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(value = 0, method = "classLoaderId")
    private IgniteUuid clsLdrId;

    /** */
    @Order(value = 1, method = "deployMode")
    private DeploymentMode depMode;

    /** */
    @Order(value = 2, method = "userVersion")
    private String userVer;

    /** */
    @Deprecated // Left for backward compatibility only.
    @Order(value = 3, method = "localDeploymentOwner")
    private boolean locDepOwner;

    /** Node class loader participant map. */
    @GridToStringInclude
    @Order(4)
    private Map<UUID, IgniteUuid> participants;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDeploymentInfoBean() {
        /* No-op. */
    }

    /**
     * @param clsLdrId Class loader ID.
     * @param userVer User version.
     * @param depMode Deployment mode.
     * @param participants Participants.
     */
    public GridDeploymentInfoBean(
        IgniteUuid clsLdrId,
        String userVer,
        DeploymentMode depMode,
        Map<UUID, IgniteUuid> participants
    ) {
        this.clsLdrId = clsLdrId;
        this.depMode = depMode;
        this.userVer = userVer;
        this.participants = participants;
    }

    /**
     * @param dep Grid deployment.
     */
    public GridDeploymentInfoBean(GridDeploymentInfo dep) {
        clsLdrId = dep.classLoaderId();
        depMode = dep.deployMode();
        userVer = dep.userVersion();
        participants = dep.participants();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /** */
    public void classLoaderId(IgniteUuid clsLdrId) {
        this.clsLdrId = clsLdrId;
    }

    /** {@inheritDoc} */
    @Override public DeploymentMode deployMode() {
        return depMode;
    }

    /** */
    public void deployMode(DeploymentMode depMode) {
        this.depMode = depMode;
    }

    /** {@inheritDoc} */
    @Override public String userVersion() {
        return userVer;
    }

    /** */
    public void userVersion(String userVer) {
        this.userVer = userVer;
    }

    /** {@inheritDoc} */
    @Override public long sequenceNumber() {
        return clsLdrId.localId();
    }

    /** {@inheritDoc} */
    @Override public boolean localDeploymentOwner() {
        return locDepOwner;
    }

    /** */
    public void localDeploymentOwner(boolean locDepOwner) {
        this.locDepOwner = locDepOwner;
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, IgniteUuid> participants() {
        return participants;
    }

    /** */
    public void participants(Map<UUID, IgniteUuid> participants) {
        this.participants = participants;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return clsLdrId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return o == this || o instanceof GridDeploymentInfoBean &&
            clsLdrId.equals(((GridDeploymentInfoBean)o).clsLdrId);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeIgniteUuid(out, clsLdrId);
        U.writeEnum(out, depMode);
        U.writeString(out, userVer);
        out.writeBoolean(locDepOwner);
        U.writeMap(out, participants);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        clsLdrId = U.readIgniteUuid(in);
        depMode = DeploymentMode.fromOrdinal(in.readByte());
        userVer = U.readString(in);
        locDepOwner = in.readBoolean();
        participants = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentInfoBean.class, this);
    }
}
