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

package org.apache.ignite.platform;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * Test invoke methods with collections and arrays as arguments and return type.
 */
public class PlatformServiceCallCollectionsTask extends AbstractPlatformServiceCallTask {
    /** {@inheritDoc} */
    @Override ComputeJobAdapter createJob(String svcName) {
        return new PlatformServiceCallCollectionsJob(svcName);
    }

    /** */
    public static class PlatformServiceCallCollectionsJob extends AbstractServiceCallJob {
        /**
         * @param svcName Service name.
         */
        PlatformServiceCallCollectionsJob(String svcName) {
            super(svcName);
        }

        /** {@inheritDoc} */
        @Override void runTest() {
            TestPlatformService srv = serviceProxy();

            {
                TestValue[] exp = IntStream.range(0, 10).mapToObj(i -> new TestValue(i, "name_" + i))
                        .toArray(TestValue[]::new);

                TestValue[] res = srv.addOneToEach(exp);

                assertEquals(exp.length, res.length);

                for (int i = 0; i < exp.length; i++)
                    assertEquals(exp[i].id() + 1, res[i].id());
            }

            {
                List<TestValue> exp = IntStream.range(0, 10).mapToObj(i -> new TestValue(i, "name_" + i))
                        .collect(Collectors.toList());

                Collection<TestValue> res = srv.addOneToEachCollection(exp);

                assertEquals(exp.size(), res.size());

                res.forEach(v -> assertEquals(exp.get(v.id() - 1).name(), v.name()));
            }

            {
                Map<TestKey, TestValue> exp = IntStream.range(0, 10)
                        .mapToObj(i -> new T2<>(new TestKey(i), new TestValue(i, "name_" + i)))
                        .collect(Collectors.toMap(T2::getKey, T2::getValue));

                Map<TestKey, TestValue> res = srv.addOneToEachDictionary(exp);

                assertEquals(exp.size(), res.size());

                res.forEach((k, v) -> assertEquals(exp.get(new TestKey(k.id() - 1)).name(), v.name()));
            }
        }
    }
}
