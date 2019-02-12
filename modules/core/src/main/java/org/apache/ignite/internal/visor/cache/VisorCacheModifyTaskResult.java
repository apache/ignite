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
