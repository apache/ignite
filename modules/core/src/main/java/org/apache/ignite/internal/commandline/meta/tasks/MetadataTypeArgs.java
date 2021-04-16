/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.meta.tasks;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class MetadataTypeArgs extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Type name argument. */
    public static final String TYPE_NAME = "--typeName";

    /** Type ID argument. */
    public static final String TYPE_ID = "--typeId";

    /** Config. */
    private String typeName;

    /** Metrics. */
    private Integer typeId;

    /**
     * Default constructor.
     */
    public MetadataTypeArgs() {
        // No-op.
    }

    /** */
    public MetadataTypeArgs(String typeName, Integer typeId) {
        assert typeName != null ^ typeId != null;

        this.typeName = typeName;
        this.typeId = typeId;
    }

    /** */
    public String typeName() {
        return typeName;
    }

    /** */
    public int typeId(GridKernalContext ctx) {
        if (typeId != null)
            return typeId;
        else
            return ctx.cacheObjects().typeId(typeName);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(typeName != null);

        if (typeName != null)
            U.writeString(out, typeName);
        else
            out.writeInt(typeId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        boolean useName = in.readBoolean();

        if (useName)
            typeName = U.readString(in);
        else
            typeId = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return typeId != null ? printInt(typeId) : typeName;
    }

    /** */
    private String printInt(int val) {
        return "0x" + Integer.toHexString(val).toUpperCase() + " (" + val + ')';
    }
}
