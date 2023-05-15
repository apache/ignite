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

package org.apache.ignite.internal.management.diagnostic;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.diagnostic.Operation;
import static org.apache.ignite.internal.visor.diagnostic.Operation.DUMP_LOG;

/** */
@ArgumentGroup(value = {"all", "nodes"}, onlyOneOf = true)
public class DiagnosticPagelocksCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(optional = true)
    @Positional
    @EnumDescription(
        names = {
            "DUMP",
            "DUMP_LOG"
        },
        descriptions = {
            "Save page locks dump to file generated in IGNITE_HOME/work/diagnostic directory",
            "Print page locks dump to console"
        }
    )
    private Operation operation = DUMP_LOG;

    /** */
    @Argument(optional = true)
    private String path;

    /** Run command for all nodes. */
    @Argument(
        description = "Run for all nodes",
        optional = true
    )
    private boolean all;

    /** */
    @Argument(
        optional = true,
        description = "Comma separated list of node ids or consistent ids",
        example = "node_id1[,node_id2....node_idN]|consistend_id1[,consistent_id2,....,consistent_idN]"
    )
    private String[] nodeIds;

    /** */
    private void ensureOperationAndPath(Operation op, String path) {
        if (path != null && op == DUMP_LOG)
            throw new IllegalArgumentException("Path can be specified only in DUMP mode.");
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, operation);
        U.writeString(out, path);
        out.writeBoolean(all);
        U.writeArray(out, nodeIds);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        operation = U.readEnum(in, Operation.class);
        path = U.readString(in);
        all = in.readBoolean();
        nodeIds = U.readArray(in, String.class);
    }

    /** */
    public String[] nodeIds() {
        return nodeIds;
    }

    /** */
    public void nodeIds(String[] nodeIds) {
        this.nodeIds = nodeIds;
    }

    /** */
    public Operation operation() {
        return operation;
    }

    /** */
    public void operation(Operation op) {
        ensureOperationAndPath(op, path);

        this.operation = op;
    }

    /** */
    public String path() {
        return path;
    }

    /** */
    public void path(String path) {
        ensureOperationAndPath(operation, path);

        this.path = path;
    }

    /** */
    public boolean all() {
        return all;
    }

    /** */
    public void all(boolean all) {
        this.all = all;
    }
}
