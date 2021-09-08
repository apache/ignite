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
public class VisorWalTaskArg extends VisorDataTransferObject {
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
