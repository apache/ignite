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

package org.apache.ignite.internal;

import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

public class TestCollectionsMessage implements Message {
    @Order(0)
    private List<boolean[]> booleanArrayList;

    @Order(1)
    private List<byte[]> byteArrayList;

    @Order(2)
    private List<short[]> shortArrayList;

    @Order(3)
    private List<int[]> intArrayList;

    @Order(4)
    private List<long[]> longArrayList;

    @Order(5)
    private List<char[]> charArrayList;

    @Order(6)
    private List<float[]> floatArrayList;

    @Order(7)
    private List<double[]> doubleArrayList;

    @Order(8)
    private List<String> stringList;

    @Order(9)
    private List<UUID> uuidList;

    @Order(10)
    private List<BitSet> bitSetList;

    @Order(11)
    private List<IgniteUuid> igniteUuidList;

    @Order(12)
    private List<AffinityTopologyVersion> affTopVersionList;

    @Order(13)
    private List<Boolean> boxedBooleanList;

    @Order(14)
    private List<Byte> boxedByteList;

    @Order(15)
    private List<Short> boxedShortList;

    @Order(16)
    private List<Integer> boxedIntList;

    @Order(17)
    private List<Long> boxedLongList;

    @Order(18)
    private List<Character> boxedCharList;

    @Order(19)
    private List<Float> boxedFloatList;

    @Order(20)
    private List<Double> boxedDoubleList;

    @Order(21)
    private List<GridCacheVersion> messageList;

    @Order(22)
    private List<GridLongList> gridLongListList;

    public List<boolean[]> booleanArrayList() {
        return booleanArrayList;
    }

    public void booleanArrayList(List<boolean[]> booleanArrayList) {
        this.booleanArrayList = booleanArrayList;
    }

    public List<byte[]> byteArrayList() {
        return byteArrayList;
    }

    public void byteArrayList(List<byte[]> byteArrayList) {
        this.byteArrayList = byteArrayList;
    }

    public List<short[]> shortArrayList() {
        return shortArrayList;
    }

    public void shortArrayList(List<short[]> shortArrayList) {
        this.shortArrayList = shortArrayList;
    }

    public List<int[]> intArrayList() {
        return intArrayList;
    }

    public void intArrayList(List<int[]> intArrayList) {
        this.intArrayList = intArrayList;
    }

    public List<long[]> longArrayList() {
        return longArrayList;
    }

    public void longArrayList(List<long[]> longArrayList) {
        this.longArrayList = longArrayList;
    }

    public List<char[]> charArrayList() {
        return charArrayList;
    }

    public void charArrayList(List<char[]> charArrayList) {
        this.charArrayList = charArrayList;
    }

    public List<float[]> floatArrayList() {
        return floatArrayList;
    }

    public void floatArrayList(List<float[]> floatArrayList) {
        this.floatArrayList = floatArrayList;
    }

    public List<double[]> doubleArrayList() {
        return doubleArrayList;
    }

    public void doubleArrayList(List<double[]> doubleArrayList) {
        this.doubleArrayList = doubleArrayList;
    }

    public List<String> stringList() {
        return stringList;
    }

    public void stringList(List<String> stringList) {
        this.stringList = stringList;
    }

    public List<UUID> uuidList() {
        return uuidList;
    }

    public void uuidList(List<UUID> uuidList) {
        this.uuidList = uuidList;
    }

    public List<BitSet> bitSetList() {
        return bitSetList;
    }

    public void bitSetList(List<BitSet> bitSetList) {
        this.bitSetList = bitSetList;
    }

    public List<IgniteUuid> igniteUuidList() {
        return igniteUuidList;
    }

    public void igniteUuidList(List<IgniteUuid> igniteUuidList) {
        this.igniteUuidList = igniteUuidList;
    }

    public List<AffinityTopologyVersion> affTopVersionList() {
        return affTopVersionList;
    }

    public void affTopVersionList(List<AffinityTopologyVersion> affTopVerList) {
        affTopVersionList = affTopVerList;
    }

    public List<Boolean> boxedBooleanList() {
        return boxedBooleanList;
    }

    public void boxedBooleanList(List<Boolean> boxedBooleanList) {
        this.boxedBooleanList = boxedBooleanList;
    }

    public List<Byte> boxedByteList() {
        return boxedByteList;
    }

    public void boxedByteList(List<Byte> boxedByteList) {
        this.boxedByteList = boxedByteList;
    }

    public List<Short> boxedShortList() {
        return boxedShortList;
    }

    public void boxedShortList(List<Short> boxedShortList) {
        this.boxedShortList = boxedShortList;
    }

    public List<Integer> boxedIntList() {
        return boxedIntList;
    }

    public void boxedIntList(List<Integer> boxedIntList) {
        this.boxedIntList = boxedIntList;
    }

    public List<Long> boxedLongList() {
        return boxedLongList;
    }

    public void boxedLongList(List<Long> boxedLongList) {
        this.boxedLongList = boxedLongList;
    }

    public List<Character> boxedCharList() {
        return boxedCharList;
    }

    public void boxedCharList(List<Character> boxedCharList) {
        this.boxedCharList = boxedCharList;
    }

    public List<Float> boxedFloatList() {
        return boxedFloatList;
    }

    public void boxedFloatList(List<Float> boxedFloatList) {
        this.boxedFloatList = boxedFloatList;
    }

    public List<Double> boxedDoubleList() {
        return boxedDoubleList;
    }

    public void boxedDoubleList(List<Double> boxedDoubleList) {
        this.boxedDoubleList = boxedDoubleList;
    }

    public List<GridCacheVersion> messageList() {
        return messageList;
    }

    public void messageList(List<GridCacheVersion> messageList) {
        this.messageList = messageList;
    }

    public List<GridLongList> gridLongListList() {
        return gridLongListList;
    }

    public void gridLongListList(List<GridLongList> gridLongListList) {
        this.gridLongListList = gridLongListList;
    }

    public short directType() {
        return 0;
    }
}
