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

package org.apache.ignite.internal.visor.misc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Argument for {@link VisorWalTask}.
 */
public class VisorWalTaskArg extends VisorDataTransferObject{
    /** */
    private static final long serialVersionUID = 0L;

    /** WAL task operation. */
    private VisorWalTaskOperation op;

    /** List of nodes' consistent ids. */
    private List<String> consistentIds;

    /**
     * Default constructor.
     */
    public VisorWalTaskArg() {
        // No-op.
    }

    /**
     * @param op Task operation.
     */
    public VisorWalTaskArg(VisorWalTaskOperation op) {
        this.op = op;
    }

    /**
     * @param op WAL task operation.
     * @param consistentIds Nodes consistent ids.
     */
    public VisorWalTaskArg(VisorWalTaskOperation op, List<String> consistentIds) {
        this.op = op;
        this.consistentIds = consistentIds;
    }

    /**
     *  Get WAL task operation.
     *
     *  @return WAL task operation.
     */
    public VisorWalTaskOperation getOperation() {
        return op == null ? VisorWalTaskOperation.PRINT_UNUSED_WAL_SEGMENTS : op;
    }

    /**
     * Get nodes consistent ids.
     *
     * @return Consistent ids.
     */
    public List<String> getConsistentIds() {
        return consistentIds;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, op);
        U.writeCollection(out, consistentIds);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        op = VisorWalTaskOperation.fromOrdinal(in.readByte());
        consistentIds = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorWalTaskArg.class, this);
    }
}
