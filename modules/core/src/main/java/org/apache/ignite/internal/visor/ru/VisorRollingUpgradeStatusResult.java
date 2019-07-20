/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.ru;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.ru.RollingUpgradeStatus;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Represents an extended rolling upgrade state that includes additional parameters, like as follows:
 * list of node IDs that are (not) updated, etc.
 */
public class VisorRollingUpgradeStatusResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private VisorRollingUpgradeStatus status;

    /** List of node IDs that are alive and not updated yet.*/
    private List<String> initNodes;

    /** List of node IDs that are alive and have been updated. */
    private List<String> updatedNodes;

    /** Default constructor. */
    public VisorRollingUpgradeStatusResult() {
    }

    /**
     * Creates a new instance of VisorRollingUpgradeStatusResult with the given parameters.
     *
     * @param status Rolling upgrade status.
     * @param initNodes List of node IDs that are alive and not updated yet.
     * @param updatedNodes List of node IDs that are alive and have been updated.
     */
    public VisorRollingUpgradeStatusResult(
        VisorRollingUpgradeStatus status,
        List<String> initNodes,
        List<String> updatedNodes
    ) {
        this.status = status;
        this.initNodes = initNodes;
        this.updatedNodes = updatedNodes;
    }

    /**
     * Returns rolling upgrade status.
     *
     * @return Rolling upgrade status.
     */
    public VisorRollingUpgradeStatus getStatus() {
        return status;
    }

    /**
     * Returns a list of node IDs that are alive and not updated yet.
     *
     * @return List of node IDs.
     */
    public List<String> getInitialNodes() {
        return initNodes;
    }

    /**
     * Returns a list of node IDs that are alive and have been updated.
     * The returned collection can be empty if Rolling Upgrade is disabled or
     * {@link RollingUpgradeStatus#targetVersion()} is not determined yet.
     *
     * This method makes sense only for the case when the {@code forced} mode is disabled.
     *
     * @return List of node IDs.
     */
    public List<String> getUpdatedNodes() {
        return updatedNodes;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(status);
        U.writeCollection(out, initNodes);
        U.writeCollection(out, updatedNodes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        status = (VisorRollingUpgradeStatus)in.readObject();
        initNodes = U.readList(in);
        updatedNodes = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorRollingUpgradeStatusResult.class, this);
    }
}
