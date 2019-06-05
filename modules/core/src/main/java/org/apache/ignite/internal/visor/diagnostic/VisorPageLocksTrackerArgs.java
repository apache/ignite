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
    private String op;
    /** */
    private String type;
    /** */
    private String filePath;
    /** */
    @Nullable private Set<String> consistentIds;

    public VisorPageLocksTrackerArgs() {

    }

    public VisorPageLocksTrackerArgs(String op, String type, String filePath, Set<String> consistentIds) {
        this.op = op;
        this.type = type;
        this.filePath = filePath;
        this.consistentIds = consistentIds;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        if (op == null)
            out.writeInt(0);
        else {
            byte[] bytes = op.getBytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        if (type == null)
            out.writeInt(0);
        else {
            byte[] bytes = type.getBytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        if (filePath == null)
            out.writeInt(0);
        else {
            byte[] bytes = filePath.getBytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        if (consistentIds != null)
            U.writeCollection(out, consistentIds);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        int opLenght = in.readInt();

        if (opLenght != 0) {
            byte[] bytes = new byte[opLenght];

            in.read(bytes);

            op = new String(bytes);
        }

        int typeLenght = in.readInt();

        if (typeLenght != 0) {
            byte[] bytes = new byte[typeLenght];

            in.read(bytes);

            type = new String(bytes);
        }

        int filePathLenght = in.readInt();

        if (filePathLenght != 0) {
            byte[] bytes = new byte[filePathLenght];

            in.read(bytes);

            filePath = new String(bytes);
        }

        consistentIds = U.readSet(in);
    }

    public String operation() {
        return op;
    }

    public String type() {
        return type;
    }

    public String filePath() {
        return filePath;
    }

    public Set<String> nodeIds(){
        return Collections.unmodifiableSet(consistentIds);
    }
}
