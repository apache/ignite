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
package org.apache.ignite.internal.management.tx;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Encapsulates info about transaction key and its lock ownership for --tx --info output.
 */
public class TxVerboseKey extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Tx key. */
    @Order(value = 0)
    String txKey;

    /** Lock type. */
    @Order(value = 1)
    TxKeyLockType lockType;

    /** Owner version. */
    @Order(value = 2)
    GridCacheVersion ownerVer;

    /** Is read entry. */
    @Order(value = 3)
    boolean read;

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
    @Override public String toString() {
        return S.toString(TxVerboseKey.class, this);
    }
}
