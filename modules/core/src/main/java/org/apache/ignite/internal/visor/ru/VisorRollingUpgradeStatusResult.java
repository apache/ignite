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

/**
 * Represents an extended rolling upgrade state that includes additional parameters, like as follows:
 * list of node IDs that are (not) updated, etc.
 */
public class VisorRollingUpgradeStatusResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private RollingUpgradeStatus status;

    /** List of node IDs that are alive and not updated yet.*/
    private List<String> initVerNodes;

    /** List of node IDs that are alive and have been updated. */
    private List<String> updateVerNodes;

    /** Default construsctor. */
    public VisorRollingUpgradeStatusResult() {
    }

    /**
     * Creates a new instance of VisorRollingUpgradeStatusResult with the given parameters.
     *
     * @param status Rolling upgrade status.
     * @param initVerNodes List of node IDs that are alive and not updated yet.
     * @param updateVerNodes List of node IDs that are alive and have been updated.
     */
    public VisorRollingUpgradeStatusResult(
        RollingUpgradeStatus status,
        List<String> initVerNodes,
        List<String> updateVerNodes
    ) {
        this.status = status;
        this.initVerNodes = initVerNodes;
        this.updateVerNodes = updateVerNodes;
    }

    /**
     * Returns rolling upgrade status.
     *
     * @return Rolling upgrade status.
     */
    public RollingUpgradeStatus status() {
        return status;
    }

    /**
     * Returns a list of node IDs that are alive and not updated yet.
     *
     * @return List of node IDs.
     */
    public List<String> initialVerNodes() {
        return initVerNodes;
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
    public List<String> updateVerNodes() {
        return updateVerNodes;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(status);
        out.writeObject(initVerNodes);
        out.writeObject(updateVerNodes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        status = (RollingUpgradeStatus)in.readObject();
        initVerNodes = (List<String>)in.readObject();
        updateVerNodes = (List<String>)in.readObject();
    }
}
