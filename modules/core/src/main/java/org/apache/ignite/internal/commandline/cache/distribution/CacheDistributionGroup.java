/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.internal.commandline.cache.distribution;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * DTO for CacheDistributionTask, contains information about group
 */
public class CacheDistributionGroup extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Group identifier. */
    private int grpId;

    /** Group name. */
    private String grpName;

    /** List of partitions. */
    private List<CacheDistributionPartition> partitions;

    /** Default constructor. */
    public CacheDistributionGroup() {
    }

    /**
     * @param grpId Group identifier.
     * @param grpName Group name.
     * @param partitions List of partitions.
     */
    public CacheDistributionGroup(int grpId, String grpName, List<CacheDistributionPartition> partitions) {
        this.grpId = grpId;
        this.grpName = grpName;
        this.partitions = partitions;
    }

    /** */
    public int getGroupId() {
        return grpId;
    }

    /** */
    public void setGroupId(int grpId) {
        this.grpId = grpId;
    }

    /** */
    public String getGroupName() {
        return grpName;
    }

    /** */
    public void setGroupName(String grpName) {
        this.grpName = grpName;
    }

    /** */
    public List<CacheDistributionPartition> getPartitions() {
        return partitions;
    }

    /** */
    public void setPartitions(
        List<CacheDistributionPartition> partitions) {
        this.partitions = partitions;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(grpId);
        U.writeString(out, grpName);
        U.writeCollection(out, partitions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        grpId = in.readInt();
        grpName = U.readString(in);
        partitions = U.readList(in);
    }
}
