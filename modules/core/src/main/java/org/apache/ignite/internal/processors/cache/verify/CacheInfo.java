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

package org.apache.ignite.internal.processors.cache.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.internal.visor.verify.VisorViewCacheCmd;

/**
 * Cache info DTO.
 */
public class CacheInfo extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Sequence name. */
    private String seqName;

    /** Sequence value. */
    private long seqVal;

    /** Cache name. */
    private String cacheName;

    /** Cache id. */
    private int cacheId;

    /** Group name. */
    private String grpName;

    /** Group id. */
    private int grpId;

    /** Caches count. */
    private int cachesCnt;

    /** Partitions. */
    private int partitions;

    /** Mapped. */
    private int mapped;

    /** Topology version. */
    public AffinityTopologyVersion topVer;

    /** Mode. */
    private CacheMode mode;

    /** Atomicity mode. */
    private CacheAtomicityMode atomicityMode;

    /** Backups count. */
    private int backupsCnt;

    /** Affinity class name. */
    private String affinityClsName;

    /** */
    public String getSeqName() {
        return seqName;
    }

    /**
     * @param seqName Sequence name.
     */
    public void setSeqName(String seqName) {
        this.seqName = seqName;
    }

    /**
     *
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     */
    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     *
     */
    public int getCacheId() {
        return cacheId;
    }

    /**
     * @param cacheId Cache id.
     */
    public void setCacheId(int cacheId) {
        this.cacheId = cacheId;
    }

    /**
     *
     */
    public String getGrpName() {
        return grpName;
    }

    /**
     * @param grpName Group name.
     */
    public void setGrpName(String grpName) {
        this.grpName = grpName;
    }

    /**
     *
     */
    public int getGrpId() {
        return grpId;
    }

    /**
     * @param grpId Group id.
     */
    public void setGrpId(int grpId) {
        this.grpId = grpId;
    }

    /**
     *
     */
    public int getCachesCnt() {
        return cachesCnt;
    }

    /**
     * @param cachesCnt Caches count.
     */
    public void setCachesCnt(int cachesCnt) {
        this.cachesCnt = cachesCnt;
    }

    /**
     *
     */
    public int getPartitions() {
        return partitions;
    }

    /**
     * @param partitions Partitions.
     */
    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    /**
     *
     */
    public int getMapped() {
        return mapped;
    }

    /**
     * @param mapped Mapped.
     */
    public void setMapped(int mapped) {
        this.mapped = mapped;
    }

    /**
     *
     */
    public AffinityTopologyVersion getTopologyVersion() {
        return topVer;
    }

    /**
     * @param topologyVersion Topology version.
     */
    public void setTopologyVersion(AffinityTopologyVersion topologyVersion) {
        this.topVer = topologyVersion;
    }

    /**
     * @param seqVal Sequence value.
     */
    public void setSeqVal(long seqVal) {
        this.seqVal = seqVal;
    }

    /**
     *
     */
    public long getSeqVal() {
        return seqVal;
    }

    /**
     *
     */
    public CacheMode getMode() {
        return mode;
    }

    /**
     * @param mode Mode.
     */
    public void setMode(CacheMode mode) {
        this.mode = mode;
    }

    /**
     *
     */
    public CacheAtomicityMode getAtomicityMode() {
        return atomicityMode;
    }

    /**
     * @param atomicityMode Atomicity mode.
     */
    public void setAtomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;
    }

    /**
     *
     */
    public int getBackupsCnt() {
        return backupsCnt;
    }

    /**
     * @param backupsCnt Backups count.
     */
    public void setBackupsCnt(int backupsCnt) {
        this.backupsCnt = backupsCnt;
    }

    /**
     *
     */
    public String getAffinityClsName() {
        return affinityClsName;
    }

    /**
     * @param affinityClsName Affinity class name.
     */
    public void setAffinityClsName(String affinityClsName) {
        this.affinityClsName = affinityClsName;
    }

    /**
     * Gets name of info for multi line output depending on cache command.
     *
     * @param cmd Cache command.
     * @return Header.
     */
    public Object name(VisorViewCacheCmd cmd) {
        switch (cmd) {
            case CACHES:
                return getCacheName();

            case GROUPS:
                return getGrpName();

            case SEQ:
                return getSeqName();

            default:
                throw new IllegalArgumentException("Unknown cache subcommand " + cmd);
        }
    }

    /**
     * @param cmd Command.
     */
    public Map<String, Object> toMap(VisorViewCacheCmd cmd) {
        Map<String, Object> map;

        switch (cmd) {
            case SEQ:
                map = new LinkedHashMap<>(2);

                map.put("seqName", getSeqName());
                map.put("curVal", seqVal);

                break;

            case GROUPS:
                map = new LinkedHashMap<>(10);

                map.put("grpName", getGrpName());
                map.put("grpId", getGrpId());
                map.put("cachesCnt", getCachesCnt());
                map.put("prim", getPartitions());
                map.put("mapped", getMapped());
                map.put("mode", getMode());
                map.put("atomicity", getAtomicityMode());
                map.put("backups", getBackupsCnt());
                map.put("affCls", getAffinityClsName());

                break;

            default:
                map = new LinkedHashMap<>(10);

                map.put("cacheName", getCacheName());
                map.put("cacheId", getCacheId());
                map.put("grpName", getGrpName());
                map.put("grpId", getGrpId());
                map.put("prim", getPartitions());
                map.put("mapped", getMapped());
                map.put("mode", getMode());
                map.put("atomicity", getAtomicityMode());
                map.put("backups", getBackupsCnt());
                map.put("affCls", getAffinityClsName());
        }

        return map;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, seqName);
        out.writeLong(seqVal);
        U.writeString(out, cacheName);
        out.writeInt(cacheId);
        U.writeString(out, grpName);
        out.writeInt(grpId);
        out.writeInt(partitions);
        out.writeInt(mapped);
        out.writeObject(topVer);
        U.writeEnum(out, mode);
        out.writeInt(backupsCnt);
        U.writeString(out, affinityClsName);
        out.writeInt(cachesCnt);
        U.writeEnum(out, atomicityMode);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        seqName = U.readString(in);
        seqVal = in.readLong();
        cacheName = U.readString(in);
        cacheId = in.readInt();
        grpName = U.readString(in);
        grpId = in.readInt();
        partitions = in.readInt();
        mapped = in.readInt();
        topVer = (AffinityTopologyVersion)in.readObject();
        mode = CacheMode.fromOrdinal(in.readByte());
        backupsCnt = in.readInt();
        affinityClsName = U.readString(in);
        cachesCnt = in.readInt();
        atomicityMode = protoVer >= V2 ? CacheAtomicityMode.fromOrdinal(in.readByte()) : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheInfo.class, this);
    }
}
