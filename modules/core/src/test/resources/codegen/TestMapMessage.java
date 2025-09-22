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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

public class TestMapMessage implements Message {
    @Order(0)
    private Map<boolean[], Long> booleanArrayBoxedLongMap;

    @Order(1)
    private Map<byte[], boolean[]> byteArrayBooleanArrayMap;

    @Order(2)
    private Map<short[], byte[]> shortArrayByteArrayMap;

    @Order(3)
    private Map<int[], short[]> intArrayShortArrayMap;

    @Order(4)
    private Map<long[], int[]> longArrayIntArrayMap;

    @Order(5)
    private Map<char[], long[]> charArrayLongArrayMap;

    @Order(6)
    private Map<float[], char[]> floatArrayCharArrayMap;

    @Order(7)
    private Map<double[], float[]> doubleArrayFloatArrayMap;

    @Order(8)
    private Map<String, double[]> stringDoubleArrayMap;

    @Order(9)
    private Map<UUID, String> uuidStringMap;

    @Order(10)
    private Map<BitSet, UUID> bitSetUuidMap;

    @Order(11)
    private Map<IgniteUuid, BitSet> igniteUuidBitSetMap;

    @Order(12)
    private Map<AffinityTopologyVersion, IgniteUuid> affTopVersionIgniteUuidMap;

    @Order(13)
    private Map<Boolean, AffinityTopologyVersion> boxedBooleanAffTopVersionMap;

    @Order(14)
    private Map<Byte, Boolean> boxedByteBoxedBooleanMap;

    @Order(15)
    private Map<Short, Byte> boxedShortBoxedByteMap;

    @Order(16)
    private Map<Integer, Short> boxedIntBoxedShortMap;

    @Order(17)
    private Map<Long, Integer> boxedLongBoxedIntMap;

    @Order(18)
    private Map<Character, Long> boxedCharBoxedLongMap;

    @Order(19)
    private Map<Float, Character> boxedFloatBoxedCharMap;

    @Order(20)
    private Map<Double, Float> boxedDoubleBoxedFloatMap;

    @Order(21)
    private Map<GridCacheVersion, Double> messageBoxedDoubleMap;

    @Order(22)
    private Map<Integer, GridLongList> integerGridLongListMap;

    @Order(23)
    private Map<GridLongList, Integer> gridLongListIntegerMap;

    public Map<boolean[], Long> booleanArrayBoxedLongMap() {
        return booleanArrayBoxedLongMap;
    }

    public void booleanArrayBoxedLongMap(Map<boolean[], Long> booleanArrayBoxedLongMap) {
        this.booleanArrayBoxedLongMap = booleanArrayBoxedLongMap;
    }

    public Map<byte[], boolean[]> byteArrayBooleanArrayMap() {
        return byteArrayBooleanArrayMap;
    }

    public void byteArrayBooleanArrayMap(Map<byte[], boolean[]> byteArrayBooleanArrayMap) {
        this.byteArrayBooleanArrayMap = byteArrayBooleanArrayMap;
    }

    public Map<short[], byte[]> shortArrayByteArrayMap() {
        return shortArrayByteArrayMap;
    }

    public void shortArrayByteArrayMap(Map<short[], byte[]> shortArrayByteArrayMap) {
        this.shortArrayByteArrayMap = shortArrayByteArrayMap;
    }

    public Map<int[], short[]> intArrayShortArrayMap() {
        return intArrayShortArrayMap;
    }

    public void intArrayShortArrayMap(Map<int[], short[]> intArrayShortArrayMap) {
        this.intArrayShortArrayMap = intArrayShortArrayMap;
    }

    public Map<long[], int[]> longArrayIntArrayMap() {
        return longArrayIntArrayMap;
    }

    public void longArrayIntArrayMap(Map<long[], int[]> longArrayIntArrayMap) {
        this.longArrayIntArrayMap = longArrayIntArrayMap;
    }

    public Map<char[], long[]> charArrayLongArrayMap() {
        return charArrayLongArrayMap;
    }

    public void charArrayLongArrayMap(Map<char[], long[]> charArrayLongArrayMap) {
        this.charArrayLongArrayMap = charArrayLongArrayMap;
    }

    public Map<float[], char[]> floatArrayCharArrayMap() {
        return floatArrayCharArrayMap;
    }

    public void floatArrayCharArrayMap(Map<float[], char[]> floatArrayCharArrayMap) {
        this.floatArrayCharArrayMap = floatArrayCharArrayMap;
    }

    public Map<double[], float[]> doubleArrayFloatArrayMap() {
        return doubleArrayFloatArrayMap;
    }

    public void doubleArrayFloatArrayMap(Map<double[], float[]> doubleArrayFloatArrayMap) {
        this.doubleArrayFloatArrayMap = doubleArrayFloatArrayMap;
    }

    public Map<String, double[]> stringDoubleArrayMap() {
        return stringDoubleArrayMap;
    }

    public void stringDoubleArrayMap(Map<String, double[]> stringDoubleArrayMap) {
        this.stringDoubleArrayMap = stringDoubleArrayMap;
    }

    public Map<UUID, String> uuidStringMap() {
        return uuidStringMap;
    }

    public void uuidStringMap(Map<UUID, String> uuidStringMap) {
        this.uuidStringMap = uuidStringMap;
    }

    public Map<BitSet, UUID> bitSetUuidMap() {
        return bitSetUuidMap;
    }

    public void bitSetUuidMap(Map<BitSet, UUID> bitSetUuidMap) {
        this.bitSetUuidMap = bitSetUuidMap;
    }

    public Map<IgniteUuid, BitSet> igniteUuidBitSetMap() {
        return igniteUuidBitSetMap;
    }

    public void igniteUuidBitSetMap(Map<IgniteUuid, BitSet> igniteUuidBitSetMap) {
        this.igniteUuidBitSetMap = igniteUuidBitSetMap;
    }

    public Map<AffinityTopologyVersion, IgniteUuid> affTopVersionIgniteUuidMap() {
        return affTopVersionIgniteUuidMap;
    }

    public void affTopVersionIgniteUuidMap(Map<AffinityTopologyVersion, IgniteUuid> affTopVersionIgniteUuidMap) {
        affTopVersionIgniteUuidMap = affTopVersionIgniteUuidMap;
    }

    public Map<Boolean, AffinityTopologyVersion> boxedBooleanAffTopVersionMap() {
        return boxedBooleanAffTopVersionMap;
    }

    public void boxedBooleanAffTopVersionMap(Map<Boolean, AffinityTopologyVersion> boxedBooleanAffTopVersionMap) {
        this.boxedBooleanAffTopVersionMap = boxedBooleanAffTopVersionMap;
    }

    public Map<Byte, Boolean> boxedByteBoxedBooleanMap() {
        return boxedByteBoxedBooleanMap;
    }

    public void boxedByteBoxedBooleanMap(Map<Byte, Boolean> boxedByteBoxedBooleanMap) {
        this.boxedByteBoxedBooleanMap = boxedByteBoxedBooleanMap;
    }

    public Map<Short, Byte> boxedShortBoxedByteMap() {
        return boxedShortBoxedByteMap;
    }

    public void boxedShortBoxedByteMap(Map<Short, Byte> boxedShortBoxedByteMap) {
        this.boxedShortBoxedByteMap = boxedShortBoxedByteMap;
    }

    public Map<Integer, Short> boxedIntBoxedShortMap() {
        return boxedIntBoxedShortMap;
    }

    public void boxedIntBoxedShortMap(Map<Integer, Short> boxedIntBoxedShortMap) {
        this.boxedIntBoxedShortMap = boxedIntBoxedShortMap;
    }

    public Map<Long, Integer> boxedLongBoxedIntMap() {
        return boxedLongBoxedIntMap;
    }

    public void boxedLongBoxedIntMap(Map<Long, Integer> boxedLongBoxedIntMap) {
        this.boxedLongBoxedIntMap = boxedLongBoxedIntMap;
    }

    public Map<Character, Long> boxedCharBoxedLongMap() {
        return boxedCharBoxedLongMap;
    }

    public void boxedCharBoxedLongMap(Map<Character, Long> boxedCharBoxedLongMap) {
        this.boxedCharBoxedLongMap = boxedCharBoxedLongMap;
    }

    public Map<Float, Character> boxedFloatBoxedCharMap() {
        return boxedFloatBoxedCharMap;
    }

    public void boxedFloatBoxedCharMap(Map<Float, Character> boxedFloatBoxedCharMap) {
        this.boxedFloatBoxedCharMap = boxedFloatBoxedCharMap;
    }

    public Map<Double, Float> boxedDoubleBoxedFloatMap() {
        return boxedDoubleBoxedFloatMap;
    }

    public void boxedDoubleBoxedFloatMap(Map<Double, Float> boxedDoubleBoxedFloatMap) {
        this.boxedDoubleBoxedFloatMap = boxedDoubleBoxedFloatMap;
    }

    public Map<GridCacheVersion, Double> messageBoxedDoubleMap() {
        return messageBoxedDoubleMap;
    }

    public void messageBoxedDoubleMap(Map<GridCacheVersion, Double> messageBoxedDoubleMap) {
        this.messageBoxedDoubleMap = messageBoxedDoubleMap;
    }

    public Map<Integer, GridLongList> integerGridLongListMap() {
        return integerGridLongListMap;
    }

    public void integerGridLongListMap(Map<Integer, GridLongList> integerGridLongListMap) {
        this.integerGridLongListMap = integerGridLongListMap;
    }

    public Map<GridLongList, Integer> gridLongListIntegerMap() {
        return gridLongListIntegerMap;
    }

    public void gridLongListIntegerMap(Map<GridLongList, Integer> gridLongListIntegerMap) {
        this.gridLongListIntegerMap = gridLongListIntegerMap;
    }

    public short directType() {
        return 0;
    }
}
