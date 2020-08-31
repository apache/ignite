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

package org.apache.ignite.internal.visor.diagnostic;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class VisorPageLocksTrackerArgs extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Operation op;

    /** */
    private String filePath;

    /** */
    @Nullable private Set<String> nodeIds;

    /** */
    public VisorPageLocksTrackerArgs() {

    }

    /**
     * @param op Operation.
     * @param filePath File path.
     * @param nodeIds Set of ids.
     */
    public VisorPageLocksTrackerArgs(Operation op, String filePath, Set<String> nodeIds) {
        this.op = op;
        this.filePath = filePath;
        this.nodeIds = nodeIds;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(op);

        U.writeString(out,filePath);

        U.writeCollection(out, nodeIds);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        op = (Operation)in.readObject();

        filePath = U.readString(in);

        nodeIds = U.readSet(in);
    }

    /** */
    public Operation operation() {
        return op;
    }

    /** */
    public String filePath() {
        return filePath;
    }

    /** */
    public Set<String> nodeIds() {
        return Collections.unmodifiableSet(nodeIds);
    }
}
