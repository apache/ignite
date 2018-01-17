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

package org.apache.ignite.ml.dlearn.context.local;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformers;
import org.apache.ignite.ml.dlearn.dataset.DLearnDataset;
import org.apache.ignite.ml.dlearn.utils.DLearnContextPartitionKey;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LocalDLearnContext}.
 */
public class LocalDLearnContextTest {
    /** */
    @Test
    public void testClose() {
        Map<Integer, String> data = generateTestData();

        LocalDLearnContextFactory<Integer, String> ctxFactory = new LocalDLearnContextFactory<>(data, 2);

        LocalDLearnContext<LocalDLearnPartition<Integer, String>> ctx = ctxFactory.createContext();
        Map<DLearnContextPartitionKey, Object> learningCtxMap = ctx.getLearningCtxMap();

        // context cache contains 2 partitions with initial data
        assertEquals(2, learningCtxMap.size());

        ctx.close();

        // all data were removed from context cache
        assertEquals(0, learningCtxMap.size());
    }

    /** */
    @Test
    public void testCloseDerivativeContext() {
        Map<Integer, String> data = generateTestData();

        LocalDLearnContextFactory<Integer, String> ctxFactory = new LocalDLearnContextFactory<>(data, 2);

        LocalDLearnContext<LocalDLearnPartition<Integer, String>> ctx = ctxFactory.createContext();
        Map<DLearnContextPartitionKey, Object> learningCtxMap = ctx.getLearningCtxMap();

        // context cache contains 2 partitions with initial data
        assertEquals(2, learningCtxMap.size());

        DLearnDataset<?> dataset = ctx.transform(DLearnContextTransformers.localToDataset((k, v) -> new double[0]));

        // features and rows were added into both partitions
        assertEquals(6, learningCtxMap.size());

        dataset.close();

        // features and rows were removed
        assertEquals(2, learningCtxMap.size());

        ctx.close();

        // all data were removed from context cache
        assertEquals(0, learningCtxMap.size());
    }

    /** */
    @Test
    public void testCloseBaseContext() {
        Map<Integer, String> data = generateTestData();

        LocalDLearnContextFactory<Integer, String> ctxFactory = new LocalDLearnContextFactory<>(data, 2);

        LocalDLearnContext<LocalDLearnPartition<Integer, String>> ctx = ctxFactory.createContext();
        Map<DLearnContextPartitionKey, Object> learningCtxMap = ctx.getLearningCtxMap();

        // context cache contains 2 partitions with initial data
        assertEquals(2, learningCtxMap.size());

        DLearnDataset<?> dataset = ctx.transform(DLearnContextTransformers.localToDataset((k, v) -> new double[0]));

        // features and rows were added into both partitions
        assertEquals(6, learningCtxMap.size());

        ctx.close();

        // 2 partitions with initial data were removed
        assertEquals(4, learningCtxMap.size());

        dataset.close();

        // all data were removed from context cache
        assertEquals(0, learningCtxMap.size());
    }

    /**
     * Generates list with data for test.
     *
     * @return list with data for test.
     */
    private Map<Integer, String> generateTestData() {
        Map<Integer, String> data = new HashMap<>();

        data.put(1, "TEST1");
        data.put(2, "TEST2");
        data.put(3, "TEST3");
        data.put(4, "TEST4");

        return data;
    }
}
