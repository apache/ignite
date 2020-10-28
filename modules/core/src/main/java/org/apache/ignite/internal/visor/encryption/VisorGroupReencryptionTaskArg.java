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

package org.apache.ignite.internal.visor.encryption;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Cache group reencryption task argument.
 */
public class VisorGroupReencryptionTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache group name, */
    private String grpName;

    /** Task action type. */
    private VisorGroupReencryptionActionType type;

    /** Default constructor. */
    public VisorGroupReencryptionTaskArg() {
        // No-op.
    }

    /**
     * @param grpName Cache group name.
     * @param type Task action type
     */
    public VisorGroupReencryptionTaskArg(String grpName, VisorGroupReencryptionActionType type) {
        this.grpName = grpName;
        this.type = type;
    }

    /** @return Cache group name, */
    public String groupName() {
        return grpName;
    }

    /** @return Task action type. */
    public VisorGroupReencryptionActionType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, grpName);
        U.writeEnum(out, type);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        grpName = U.readString(in);
        type = VisorGroupReencryptionActionType.fromOrdinal(in.readByte());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGroupReencryptionTaskArg.class, this);
    }
}
