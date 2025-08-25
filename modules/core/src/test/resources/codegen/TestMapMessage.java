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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

public class TestMapMessage implements Message {
    @Order(0)
    private Map<boolean[], boolean[]> booleanArrayMap;

    @Order(1)
    private Map<byte[], byte[]> byteArrayMap;

    @Order(2)
    private Map<short[], short[]> shortArrayMap;

    @Order(3)
    private Map<int[], int[]> intArrayMap;

    @Order(4)
    private Map<long[], long[]> longArrayMap;

    @Order(5)
    private Map<char[], char[]> charArrayMap;

    @Order(6)
    private Map<float[], float[]> floatArrayMap;

    @Order(7)
    private Map<double[], double[]> doubleArrayMap;

    @Order(8)
    private Map<String, String> stringMap;

    @Order(9)
    private Map<UUID, UUID> uuidMap;

    @Order(10)
    private Map<BitSet, BitSet> bitSetMap;

    @Order(11)
    private Map<IgniteUuid, IgniteUuid> igniteUuidMap;

    @Order(12)
    private Map<AffinityTopologyVersion, AffinityTopologyVersion> affTopVersionMap;

    @Order(13)
    private Map<Boolean, Boolean> boxedBooleanMap;

    @Order(14)
    private Map<Byte, Byte> boxedByteMap;

    @Order(15)
    private Map<Short, Short> boxedShortMap;

    @Order(16)
    private Map<Integer, Integer> boxedIntMap;

    @Order(17)
    private Map<Long, Long> boxedLongMap;

    @Order(18)
    private Map<Character, Character> boxedCharMap;

    @Order(19)
    private Map<Float, Float> boxedFloatMap;

    @Order(20)
    private Map<Double, Double> boxedDoubleMap;

    @Order(21)
    private Map<GridCacheVersion, GridCacheVersion> messageMap;

    @Order(22)
    private LinkedHashMap<Long, Long> linkedMap;

    public Map<boolean[], boolean[]> booleanArrayMap() {
        return booleanArrayMap;
    }

    public void booleanArrayMap(Map<boolean[], boolean[]> booleanArrayMap) {
        this.booleanArrayMap = booleanArrayMap;
    }

    public Map<byte[], byte[]> byteArrayMap() {
        return byteArrayMap;
    }

    public void byteArrayMap(Map<byte[], byte[]> byteArrayMap) {
        this.byteArrayMap = byteArrayMap;
    }

    public Map<short[], short[]> shortArrayMap() {
        return shortArrayMap;
    }

    public void shortArrayMap(Map<short[], short[]> shortArrayMap) {
        this.shortArrayMap = shortArrayMap;
    }

    public Map<int[], int[]> intArrayMap() {
        return intArrayMap;
    }

    public void intArrayMap(Map<int[], int[]> intArrayMap) {
        this.intArrayMap = intArrayMap;
    }

    public Map<long[], long[]> longArrayMap() {
        return longArrayMap;
    }

    public void longArrayMap(Map<long[], long[]> longArrayMap) {
        this.longArrayMap = longArrayMap;
    }

    public Map<char[], char[]> charArrayMap() {
        return charArrayMap;
    }

    public void charArrayMap(Map<char[], char[]> charArrayMap) {
        this.charArrayMap = charArrayMap;
    }

    public Map<float[], float[]> floatArrayMap() {
        return floatArrayMap;
    }

    public void floatArrayMap(Map<float[], float[]> floatArrayMap) {
        this.floatArrayMap = floatArrayMap;
    }

    public Map<double[], double[]> doubleArrayMap() {
        return doubleArrayMap;
    }

    public void doubleArrayMap(Map<double[], double[]> doubleArrayMap) {
        this.doubleArrayMap = doubleArrayMap;
    }

    public Map<String, String> stringMap() {
        return stringMap;
    }

    public void stringMap(Map<String, String> stringMap) {
        this.stringMap = stringMap;
    }

    public Map<UUID, UUID> uuidMap() {
        return uuidMap;
    }

    public void uuidMap(Map<UUID, UUID> uuidMap) {
        this.uuidMap = uuidMap;
    }

    public Map<BitSet, BitSet> bitSetMap() {
        return bitSetMap;
    }

    public void bitSetMap(Map<BitSet, BitSet> bitSetMap) {
        this.bitSetMap = bitSetMap;
    }

    public Map<IgniteUuid, IgniteUuid> igniteUuidMap() {
        return igniteUuidMap;
    }

    public void igniteUuidMap(Map<IgniteUuid, IgniteUuid> igniteUuidMap) {
        this.igniteUuidMap = igniteUuidMap;
    }

    public Map<AffinityTopologyVersion, AffinityTopologyVersion> affTopVersionMap() {
        return affTopVersionMap;
    }

    public void affTopVersionMap(Map<AffinityTopologyVersion, AffinityTopologyVersion> affTopVersionMap) {
        affTopVersionMap = affTopVersionMap;
    }

    public Map<Boolean, Boolean> boxedBooleanMap() {
        return boxedBooleanMap;
    }

    public void boxedBooleanMap(Map<Boolean, Boolean> boxedBooleanMap) {
        this.boxedBooleanMap = boxedBooleanMap;
    }

    public Map<Byte, Byte> boxedByteMap() {
        return boxedByteMap;
    }

    public void boxedByteMap(Map<Byte, Byte> boxedByteMap) {
        this.boxedByteMap = boxedByteMap;
    }

    public Map<Short, Short> boxedShortMap() {
        return boxedShortMap;
    }

    public void boxedShortMap(Map<Short, Short> boxedShortMap) {
        this.boxedShortMap = boxedShortMap;
    }

    public Map<Integer, Integer> boxedIntMap() {
        return boxedIntMap;
    }

    public void boxedIntMap(Map<Integer, Integer> boxedIntMap) {
        this.boxedIntMap = boxedIntMap;
    }

    public Map<Long, Long> boxedLongMap() {
        return boxedLongMap;
    }

    public void boxedLongMap(Map<Long, Long> boxedLongMap) {
        this.boxedLongMap = boxedLongMap;
    }

    public Map<Character, Character> boxedCharMap() {
        return boxedCharMap;
    }

    public void boxedCharMap(Map<Character, Character> boxedCharMap) {
        this.boxedCharMap = boxedCharMap;
    }

    public Map<Float, Float> boxedFloatMap() {
        return boxedFloatMap;
    }

    public void boxedFloatMap(Map<Float, Float> boxedFloatMap) {
        this.boxedFloatMap = boxedFloatMap;
    }

    public Map<Double, Double> boxedDoubleMap() {
        return boxedDoubleMap;
    }

    public void boxedDoubleMap(Map<Double, Double> boxedDoubleMap) {
        this.boxedDoubleMap = boxedDoubleMap;
    }

    public Map<GridCacheVersion, GridCacheVersion> messageMap() {
        return messageMap;
    }

    public void messageMap(Map<GridCacheVersion, GridCacheVersion> messageMap) {
        messageMap = messageMap;
    }

    public LinkedHashMap<Long, Long> linkedMap() {
        return linkedMap;
    }

    public void linkedMap(LinkedHashMap<Long, Long> linkedMap) {
        this.linkedMap = linkedMap;
    }

    public short directType() {
        return 0;
    }

    public void onAckReceived() {
        // No-op.
    }
}
