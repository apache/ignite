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


package org.apache.ignite.internal.processors.rollingupgrade.message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.ignite.internal.Compress;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;

/** */
public class TestContainerMessage_2_20_0 extends TestDiscoveryMessage {
    /** */
    @Order(0)
    TestCoreMessage_2_20_0 msg;

    /** */
    @Order(1)
    List<TestCoreMessage_2_20_0> list;

    /** */
    @Order(2)
    Map<Integer, TestCoreMessage_2_20_0> map;

    /** */
    @Order(3)
    TestCoreMessage_2_20_0[] arr;

    /** */
    @Compress
    @Order(4)
    TestCoreMessage_2_20_0 compressedMsg;

    /** */
    @Compress
    @Order(5)
    Map<Integer, TestCoreMessage_2_20_0> compressedMap;

    /** {@inheritDoc} */
    @Override public TestDiscoveryMessage fill(Predicate<IgniteFeature> featureStatusProvider) {
        msg = nestedMessage(featureStatusProvider);
        list = List.of(nestedMessage(featureStatusProvider));
        map = Map.of(0, nestedMessage(featureStatusProvider));
        arr = new TestCoreMessage_2_20_0[] {nestedMessage(featureStatusProvider)};
        compressedMsg = nestedMessage(featureStatusProvider);
        compressedMap = Map.of(0, nestedMessage(featureStatusProvider));

        return this;
    }

    /** {@inheritDoc} */
    @Override public List<TestMessage> nestedMessages() {
        List<TestMessage> res = new ArrayList<>();

        res.add(msg);
        res.addAll(list);
        res.addAll(map.values());
        res.addAll(List.of(arr));
        res.add(compressedMsg);
        res.addAll(compressedMap.values());

        return res;
    }

    /** */
    private static TestCoreMessage_2_20_0 nestedMessage(Predicate<IgniteFeature> featureStatusProvider) {
        return (TestCoreMessage_2_20_0)new TestCoreMessage_2_20_0().fill(featureStatusProvider);
    }
}
