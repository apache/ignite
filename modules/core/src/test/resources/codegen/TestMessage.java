package org.apache.ignite.internal;

import java.lang.String;
import java.util.UUID;
import java.util.BitSet;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

public class TestMessage implements Message {
    @Order(0)
    private int id;

    @Order(1)
    private byte[] byteArr;

    @Order(2)
    private String str;

    @Order(3)
    private String[] strArr;

    @Order(4)
    private int[][] intMatrix;

    @Order(5)
    private GridCacheVersion ver;

    @Order(6)
    private GridCacheVersion[] verArr;

    @Order(7)
    private UUID uuid;

    @Order(8)
    private IgniteUuid ignUuid;

    @Order(9)
    private AffinityTopologyVersion topVer;

    @Order(10)
    private BitSet bitSet;

    public int id() {
        return id;
    }

    public void id(int id) {
        this.id = id;
    }

    public byte[] byteArr() {
        return byteArr;
    }

    public void byteArr(byte[] byteArr) {
        this.byteArr = byteArr;
    }

    public String str() {
        return str;
    }

    public void str(String str) {
        this.str = str;
    }

    public String[] strArr() {
        return strArr;
    }

    public void strArr(String[] strArr) {
        this.strArr = strArr;
    }

    public int[][] intMatrix() {
        return intMatrix;
    }

    public void intMatrix(int[][] intMatrix) {
        this.intMatrix = intMatrix;
    }

    public GridCacheVersion ver() {
        return ver;
    }

    public void ver(GridCacheVersion ver) {
        this.ver = ver;
    }

    public GridCacheVersion[] verArr() {
        return verArr;
    }

    public void verArr(GridCacheVersion[] verArr) {
        this.verArr = verArr;
    }

    public UUID uuid() {
        return uuid;
    }

    public void uuid(UUID uuid) {
        this.uuid = uuid;
    }

    public IgniteUuid ignUuid() {
        return ignUuid;
    }

    public void ignUuid(IgniteUuid ignUuid) {
        this.ignUuid = ignUuid;
    }

    public AffinityTopologyVersion topVer() {
        return topVer;
    }

    public void topVer(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    public BitSet bitSet() {
        return bitSet;
    }

    public void bitSet(BitSet bitSet) {
        this.bitSet = bitSet;
    }

    public short directType() {
        return 0;
    }

    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return true;
    }

    public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return true;
    }

    public void onAckReceived() {
        // No-op.
    }
}
