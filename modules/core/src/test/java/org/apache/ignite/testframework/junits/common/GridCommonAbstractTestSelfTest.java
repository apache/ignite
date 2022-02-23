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
import org.junit.Test;

/**
 *
 */
public class GridCommonAbstractTestSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    @Test
    public void testOrderedCollectionsEqualityChecks() {
        assertEqualsCollections(
            Arrays.asList(1, 2, 3),
            Arrays.asList(1, 2, 3));

        asserFailed(() -> assertEqualsCollections(
            Arrays.asList(1, 2, 3),
            Arrays.asList(2, 3, 1)));

        asserFailed(() -> assertEqualsCollections(
            Arrays.asList(1, 2, 3, null),
            Arrays.asList(2, 3, 1)));

        asserFailed(() -> assertEqualsCollections(
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

        asserFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 3),
            Arrays.asList(2, 3, 1, 2)));

        asserFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 3),
            Arrays.asList(1, 2, 2, 3)));

        asserFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 2, 3),
            Arrays.asList(1, 1, 2, 3)));

        asserFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 2),
            Arrays.asList(1, 2, 2, 4)));

        asserFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 2),
            Arrays.asList(1, 2, 2, null)));

        asserFailed(() -> assertEqualsCollectionsIgnoringOrder(
            Arrays.asList(1, 2, 2, null),
            Arrays.asList(1, 2, 2)));
    }

    /**
     *
     */
    @Test
    public void testMapsEqualityChecks() {
        assertEqualsMaps(
            new HashMap<Integer, Integer>() {{
                put(1, 1);
                put(2, 2);
                put(3, 3);
            }},
            new HashMap<Integer, Integer>() {{
                put(1, 1);
                put(3, 3);
                put(2, 2);
            }});

        asserFailed(() -> assertEqualsMaps(
            new HashMap<Integer, Integer>() {{
                put(1, 1);
                put(2, 2);
                put(3, 3);
            }},
            new HashMap<Integer, Integer>() {{
                put(1, 1);
                put(2, 2);
                put(3, 3);
                put(4, 4);
            }}));

        asserFailed(() -> assertEqualsMaps(
            new HashMap<Integer, Integer>() {{
                put(1, 1);
                put(2, 2);
                put(3, 3);
                put(null, null);
            }},
            new HashMap<Integer, Integer>() {{
                put(1, 1);
                put(2, 2);
                put(3, 3);
                put(4, 4);
            }}));

        asserFailed(() -> assertEqualsMaps(
            new HashMap<Integer, Integer>() {{
                put(1, 1);
                put(2, 2);
                put(3, 3);
                put(null, null);
            }},
            new HashMap<Integer, Integer>() {{
                put(1, 1);
                put(2, 2);
                put(3, 3);
            }}));

        asserFailed(() -> assertEqualsMaps(
            new HashMap<Integer, Integer>() {{
                put(1, 1);
                put(2, 2);
                put(3, 3);
            }},
            new HashMap<Integer, Integer>() {{
                put(1, 1);
                put(2, 2);
                put(3, 3);
                put(null, null);
            }}));
    }

    /**
     * @param r Runnable.
     */
    private void asserFailed(Runnable r) {
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
