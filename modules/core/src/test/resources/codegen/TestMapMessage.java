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
    Map<boolean[], Long> booleanArrayBoxedLongMap;

    @Order(1)
    Map<byte[], boolean[]> byteArrayBooleanArrayMap;

    @Order(2)
    Map<short[], byte[]> shortArrayByteArrayMap;

    @Order(3)
    Map<int[], short[]> intArrayShortArrayMap;

    @Order(4)
    Map<long[], int[]> longArrayIntArrayMap;

    @Order(5)
    Map<char[], long[]> charArrayLongArrayMap;

    @Order(6)
    Map<float[], char[]> floatArrayCharArrayMap;

    @Order(7)
    Map<double[], float[]> doubleArrayFloatArrayMap;

    @Order(8)
    Map<String, double[]> stringDoubleArrayMap;

    @Order(9)
    Map<UUID, String> uuidStringMap;

    @Order(10)
    Map<BitSet, UUID> bitSetUuidMap;

    @Order(11)
    Map<IgniteUuid, BitSet> igniteUuidBitSetMap;

    @Order(12)
    Map<AffinityTopologyVersion, IgniteUuid> affTopVersionIgniteUuidMap;

    @Order(13)
    Map<Boolean, AffinityTopologyVersion> boxedBooleanAffTopVersionMap;

    @Order(14)
    Map<Byte, Boolean> boxedByteBoxedBooleanMap;

    @Order(15)
    Map<Short, Byte> boxedShortBoxedByteMap;

    @Order(16)
    Map<Integer, Short> boxedIntBoxedShortMap;

    @Order(17)
    Map<Long, Integer> boxedLongBoxedIntMap;

    @Order(18)
    Map<Character, Long> boxedCharBoxedLongMap;

    @Order(19)
    Map<Float, Character> boxedFloatBoxedCharMap;

    @Order(20)
    Map<Double, Float> boxedDoubleBoxedFloatMap;

    @Order(21)
    Map<GridCacheVersion, Double> messageBoxedDoubleMap;

    @Order(22)
    Map<Integer, GridLongList> integerGridLongListMap;

    @Order(23)
    Map<GridLongList, Integer> gridLongListIntegerMap;

    public short directType() {
        return 0;
    }
}
