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
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

public class TestCollectionsMessage implements Message {
    @Order(0)
    List<boolean[]> booleanArrayList;

    @Order(1)
    List<byte[]> byteArrayList;

    @Order(2)
    List<short[]> shortArrayList;

    @Order(3)
    List<int[]> intArrayList;

    @Order(4)
    List<long[]> longArrayList;

    @Order(5)
    List<char[]> charArrayList;

    @Order(6)
    List<float[]> floatArrayList;

    @Order(7)
    List<double[]> doubleArrayList;

    @Order(8)
    List<String> stringList;

    @Order(9)
    List<UUID> uuidList;

    @Order(10)
    List<BitSet> bitSetList;

    @Order(11)
    List<IgniteUuid> igniteUuidList;

    @Order(12)
    List<AffinityTopologyVersion> affTopVersionList;

    @Order(13)
    List<Boolean> boxedBooleanList;

    @Order(14)
    List<Byte> boxedByteList;

    @Order(15)
    List<Short> boxedShortList;

    @Order(16)
    List<Integer> boxedIntList;

    @Order(17)
    List<Long> boxedLongList;

    @Order(18)
    List<Character> boxedCharList;

    @Order(19)
    List<Float> boxedFloatList;

    @Order(20)
    List<Double> boxedDoubleList;

    @Order(21)
    List<GridCacheVersion> messageList;

    @Order(22)
    List<GridLongList> gridLongListList;

    @Order(23)
    Set<Integer> boxedIntegerSet;

    @Order(24)
    Set<BitSet> bitSetSet;

    public short directType() {
        return 0;
    }
}
