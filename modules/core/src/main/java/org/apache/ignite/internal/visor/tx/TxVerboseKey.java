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
package org.apache.ignite.internal.visor.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Encapsulates info about transaction key and its lock ownership for --tx --info output.
 */
public class TxVerboseKey extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Tx key. */
    private String txKey;

    /** Lock type. */
    private TxKeyLockType lockType;

    /** Owner version. */
    private GridCacheVersion ownerVer;

    /** Is read entry. */
    private boolean read;

    /**
     * Default constructor.
     */
    public TxVerboseKey() {
        // No-op.
    }

    /**
     * @param txKey Tx key.
     * @param lockType Lock type.
     * @param ownerVer Owner version.
     * @param read Read.
     */
    public TxVerboseKey(String txKey, TxKeyLockType lockType, GridCacheVersion ownerVer, boolean read) {
        this.txKey = txKey;
        this.lockType = lockType;
        this.ownerVer = ownerVer;
        this.read = read;
    }

    /**
     * @return Tx key.
     */
    public String txKey() {
        return txKey;
    }

    /**
     * @return Lock type.
     */
    public TxKeyLockType lockType() {
        return lockType;
    }

    /**
     * @return Owner version.
     */
    public GridCacheVersion ownerVersion() {
        return ownerVer;
    }

    /**
     * @return Read.
     */
    public boolean read() {
        return read;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, txKey);
        U.writeEnum(out, lockType);
        out.writeObject(ownerVer);
        out.writeBoolean(read);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        txKey = U.readString(in);
        lockType = TxKeyLockType.fromOrdinal(in.readByte());
        ownerVer = (GridCacheVersion)in.readObject();
        read = in.readBoolean();
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxVerboseKey.class, this);
    }
}
