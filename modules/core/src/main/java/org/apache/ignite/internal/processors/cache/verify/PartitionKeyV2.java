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
package org.apache.ignite.internal.processors.cache.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Partition key - pair of cache group ID and partition ID.
 */
public class PartitionKeyV2 extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Group id. */
    private int grpId;

    /** Group name. Optional field, used only for output. */
    private volatile String grpName;

    /** Partition id. */
    private int partId;

    /**
     * @param grpId Group id.
     * @param partId Partition id.
     * @param grpName Group name.
     */
    public PartitionKeyV2(int grpId, int partId, String grpName) {
        this.grpId = grpId;
        this.partId = partId;
        this.grpName = grpName;
    }

    /**
     * Default constructor for Externalizable.
     */
    public PartitionKeyV2() {
    }

    /**
     * @return Group id.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @return Partition id.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * @return Group name.
     */
    public String groupName() {
        return grpName;
    }

    /**
     * @param grpName Group name.
     */
    public void groupName(String grpName) {
        this.grpName = grpName;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(grpId);
        U.writeString(out, grpName);
        out.writeInt(partId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        grpId = in.readInt();
        grpName = U.readString(in);
        partId = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        PartitionKeyV2 key = (PartitionKeyV2)o;

        return grpId == key.grpId && partId == key.partId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = grpId;

        res = 31 * res + partId;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionKeyV2.class, this);
    }
}
