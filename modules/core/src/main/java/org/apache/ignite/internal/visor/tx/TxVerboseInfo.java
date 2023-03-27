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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Encapsulates all verbose info about transaction needed for --tx --info output.
 */
public class TxVerboseInfo extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near xid version. */
    private GridCacheVersion nearXidVer;

    /** Local node id. */
    private UUID locNodeId;

    /** Local node consistent id. */
    private Object locNodeConsistentId;

    /** Near node id. */
    private UUID nearNodeId;

    /** Near node consistent id. */
    private Object nearNodeConsistentId;

    /** Tx mapping type. */
    private TxMappingType txMappingType;

    /** Dht node id. */
    private UUID dhtNodeId;

    /** Dht node consistent id. */
    private Object dhtNodeConsistentId;

    /** Used caches. */
    private Map<Integer, String> usedCaches;

    /** Used cache groups. */
    private Map<Integer, String> usedCacheGroups;

    /** Local tx keys. */
    private List<TxVerboseKey> locTxKeys;

    /** Near only tx keys. */
    private List<TxVerboseKey> nearOnlyTxKeys;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(nearXidVer);
        U.writeUuid(out, locNodeId);
        out.writeObject(locNodeConsistentId);
        U.writeUuid(out, nearNodeId);
        out.writeObject(nearNodeConsistentId);
        U.writeEnum(out, txMappingType);
        U.writeUuid(out, dhtNodeId);
        out.writeObject(dhtNodeConsistentId);
        U.writeMap(out, usedCaches);
        U.writeMap(out, usedCacheGroups);
        U.writeCollection(out, locTxKeys);
        U.writeCollection(out, nearOnlyTxKeys);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        nearXidVer = (GridCacheVersion)in.readObject();
        locNodeId = U.readUuid(in);
        locNodeConsistentId = in.readObject();
        nearNodeId = U.readUuid(in);
        nearNodeConsistentId = in.readObject();
        txMappingType = TxMappingType.fromOrdinal(in.readByte());
        dhtNodeId = U.readUuid(in);
        dhtNodeConsistentId = in.readObject();
        usedCaches = U.readHashMap(in);
        usedCacheGroups = U.readHashMap(in);
        locTxKeys = U.readList(in);
        nearOnlyTxKeys = U.readList(in);
    }

    /**
     * @return Near xid version.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @param nearXidVer New near xid version.
     */
    public void nearXidVersion(GridCacheVersion nearXidVer) {
        this.nearXidVer = nearXidVer;
    }

    /**
     * @return Local node id.
     */
    public UUID localNodeId() {
        return locNodeId;
    }

    /**
     * @param locNodeId New local node id.
     */
    public void localNodeId(UUID locNodeId) {
        this.locNodeId = locNodeId;
    }

    /**
     * @return Local node consistent id.
     */
    public Object localNodeConsistentId() {
        return locNodeConsistentId;
    }

    /**
     * @param locNodeConsistentId New local node consistent id.
     */
    public void localNodeConsistentId(Object locNodeConsistentId) {
        this.locNodeConsistentId = locNodeConsistentId;
    }

    /**
     * @return Near node id.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @param nearNodeId New near node id.
     */
    public void nearNodeId(UUID nearNodeId) {
        this.nearNodeId = nearNodeId;
    }

    /**
     * @return Near node consistent id.
     */
    public Object nearNodeConsistentId() {
        return nearNodeConsistentId;
    }

    /**
     * @param nearNodeConsistentId New near node consistent id.
     */
    public void nearNodeConsistentId(Object nearNodeConsistentId) {
        this.nearNodeConsistentId = nearNodeConsistentId;
    }

    /**
     * @return Tx mapping type.
     */
    public TxMappingType txMappingType() {
        return txMappingType;
    }

    /**
     * @param txMappingType New tx mapping type.
     */
    public void txMappingType(TxMappingType txMappingType) {
        this.txMappingType = txMappingType;
    }

    /**
     * @return Dht node id.
     */
    public UUID dhtNodeId() {
        return dhtNodeId;
    }

    /**
     * @param dhtNodeId New dht node id.
     */
    public void dhtNodeId(UUID dhtNodeId) {
        this.dhtNodeId = dhtNodeId;
    }

    /**
     * @return Dht node consistent id.
     */
    public Object dhtNodeConsistentId() {
        return dhtNodeConsistentId;
    }

    /**
     * @param dhtNodeConsistentId New dht node consistent id.
     */
    public void dhtNodeConsistentId(Object dhtNodeConsistentId) {
        this.dhtNodeConsistentId = dhtNodeConsistentId;
    }

    /**
     * @return Used caches.
     */
    public Map<Integer, String> usedCaches() {
        return usedCaches;
    }

    /**
     * @param usedCaches New used caches.
     */
    public void usedCaches(Map<Integer, String> usedCaches) {
        this.usedCaches = usedCaches;
    }

    /**
     * @return Used cache groups.
     */
    public Map<Integer, String> usedCacheGroups() {
        return usedCacheGroups;
    }

    /**
     * @param usedCacheGroups New used cache groups.
     */
    public void usedCacheGroups(Map<Integer, String> usedCacheGroups) {
        this.usedCacheGroups = usedCacheGroups;
    }

    /**
     * @return Local tx keys.
     */
    public List<TxVerboseKey> localTxKeys() {
        return locTxKeys;
    }

    /**
     * @param locTxKeys New local tx keys.
     */
    public void localTxKeys(List<TxVerboseKey> locTxKeys) {
        this.locTxKeys = locTxKeys;
    }

    /**
     * @return Near only tx keys.
     */
    public List<TxVerboseKey> nearOnlyTxKeys() {
        return nearOnlyTxKeys;
    }

    /**
     * @param nearOnlyTxKeys New near only tx keys.
     */
    public void nearOnlyTxKeys(List<TxVerboseKey> nearOnlyTxKeys) {
        this.nearOnlyTxKeys = nearOnlyTxKeys;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxVerboseInfo.class, this);
    }
}
