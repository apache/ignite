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

package org.apache.ignite.testframework.junits.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Consumer;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.junit.Test;

import static org.apache.ignite.testframework.config.GridTestProperties.findTestResource;

/**
 *
 */
public class GridCommonAbstractTestSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    @Test
    public void testResourceLocation() {
        Consumer<String> check = (res) -> assertNotNull("Required resource not found: " + res, findTestResource(res));

        check.accept(GridTestProperties.TESTS_PROP_FILE);
        check.accept(GridTestProperties.DEFAULT_LOG4J_FILE);
    }

    /**
     *
     */
    @Test
    public void testOrderedCollectionsEqualityChecks() {
        assertEqualsCollections(
            Arrays.asList(1, 2, 3),
            Arrays.asList(1, 2, 3));

        assertFailed(() -> assertEqualsCollections(
            Arrays.asList(1, 2, 3),
            Arrays.asList(2, 3, 1)));

        assertFailed(() -> assertEqualsCollections(
            Arrays.asList(1, 2, 3, null),
            Arrays.asList(2, 3, 1)));

        assertFailed(() -> assertEqualsCollections(
            Arrays.asList(1, 2, 3),
            Arrays.asList(2, 3, 1, null)));
    }

    /**
     *
     */
    @Test
    public void testCollectionsEqualityChecks() {
        assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 3),
            Arrays.asList(1, 2, 3));

        assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 3),
            Arrays.asList(2, 3, 1));

        assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 2, 3),
            Arrays.asList(2, 3, 2, 1));

        assertFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 3),
            Arrays.asList(2, 3, 1, 2)));

        assertFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 3),
            Arrays.asList(1, 2, 2, 3)));

        assertFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 2, 3),
            Arrays.asList(1, 1, 2, 3)));

        assertFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 2),
            Arrays.asList(1, 2, 2, 4)));

        assertFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 2),
            Arrays.asList(1, 2, 2, null)));

        assertFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 2, null),
            Arrays.asList(1, 2, 2)));

        assertFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 2, 3),
            Arrays.asList(1, 2, 2, null)));

        assertFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 2, null),
            Arrays.asList(1, 2, 2, 3)));
    }

    /**
     *
     */
    @Test
    public void testMapsEqualityChecks() {
        HashMap<Integer, Integer> map1 = new HashMap<>();

        map1.put(1, 1);
        map1.put(2, 2);
        map1.put(3, 3);

        HashMap<Integer, Integer> map2 = new HashMap<>();

        map2.put(1, 1);
        map2.put(3, 3);
        map2.put(2, 2);

        assertEqualsMaps(map1, map2);

        HashMap<Integer, Integer> map3 = new HashMap<>();

        map3.put(1, 1);
        map3.put(2, 2);
        map3.put(3, 3);
        map3.put(4, 4);

        assertFailed(() -> assertEqualsMaps(map1, map3));

        HashMap<Integer, Integer> map4 = new HashMap<>();

        map4.put(1, 1);
        map4.put(2, 2);
        map4.put(3, 3);
        map4.put(null, null);

        assertFailed(() -> assertEqualsMaps(map4, map3));

        assertFailed(() -> assertEqualsMaps(map4, map1));

        assertFailed(() -> assertEqualsMaps(map1, map4));
    }

    /**
     * @param r Runnable.
     */
    private void assertFailed(Runnable r) {
        boolean failed = false;

        try {
            r.run();
        }
        catch (Throwable e) {
            assert e instanceof AssertionError;

            failed = true;
        }

        assertTrue(failed);
    }
}
