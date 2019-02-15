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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Result for {@link VisorCacheModifyTask}.
 */
public class VisorCacheModifyTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID where modified data contained. */
    private UUID affinityNode;

    /** Result type name. */
    private String resType;

    /** Value for specified key or number of modified rows. */
    private Object res;

    /**
     * Default constructor.
     */
    public VisorCacheModifyTaskResult() {
        // No-op.
    }

    /**
     * @param affinityNode Node ID where modified data contained.
     * @param resType Result type name.
     * @param res Value for specified key or number of modified rows.
     */
    public VisorCacheModifyTaskResult(UUID affinityNode, String resType, Object res) {
        this.affinityNode = affinityNode;
        this.resType = resType;
        this.res = res;
    }

    /**
     * @return Node ID where modified data contained.
     */
    public UUID getAffinityNode() {
        return affinityNode;
    }

    /**
     * @return Result type name.
     */
    public String getResultType() {
        return resType;
    }

    /**
     * @return Value for specified key or number of modified rows.
     */
    public Object getResult() {
        return res;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeUuid(out, affinityNode);
        U.writeString(out, resType);
        out.writeObject(res);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        affinityNode = U.readUuid(in);
        resType = U.readString(in);
        res = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheModifyTaskResult.class, this);
    }
}
