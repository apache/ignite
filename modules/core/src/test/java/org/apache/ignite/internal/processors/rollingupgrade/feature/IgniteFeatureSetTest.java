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

package org.apache.ignite.internal.processors.rollingupgrade.feature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class IgniteFeatureSetTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testFeatureSetElements() {
        checkFeatureSetElements(List.of(0), featureSetOf(0));
        checkFeatureSetElements(asList(0, 1, 2), featureSetOf(0, 1, 2));
        checkFeatureSetElements(asList(0, 2, 4, 6), featureSetOf(0, 2, 4, 6));
        checkFeatureSetElements(asList(0, 1, 2, 3), featureSetOf(3));
        checkFeatureSetElements(asList(0, 1, 2, 3, 4), featureSetOf(2, 3, 4));
        checkFeatureSetElements(asList(0, 10), featureSetOf(0, 10));
        checkFeatureSetElements(asList(0, 1, 2, 10), featureSetOf(0, 1, 2, 10));
        checkFeatureSetElements(asList(0, 1, 2, 3, 4, 10), featureSetOf(2, 3, 4, 10));
        checkFeatureSetElements(asList(0, 1, 2, 3, 4, 10, 15), featureSetOf(2, 3, 4, 10, 15));
        checkFeatureSetElements(asList(0, 1, 2, 3, 4, 5, 10, 15), featureSetOf(5, 2, 10, 3, 15, 4));
    }

    /** */
    @Test
    public void testNotContains() {
        IgniteFeatureSet featureSet = featureSetOf(2, 3, 4, 10, 15);

        assertFalse(featureSet.contains(5));
        assertFalse(featureSet.contains(11));
        assertFalse(featureSet.contains(16));
    }

    /** */
    @Test
    public void testUpgradeAvailability() {
        assertTrue(featureSetOf(0).isUpgradableTo(featureSetOf(0)));
        assertTrue(featureSetOf(0).isUpgradableTo(featureSetOf(0, 1, 2)));
        assertTrue(featureSetOf(0, 1, 2).isUpgradableTo(featureSetOf(0, 1, 2, 3)));
        assertTrue(featureSetOf(0, 1, 2).isUpgradableTo(featureSetOf(2, 3, 4)));
        assertTrue(featureSetOf(0, 1, 2).isUpgradableTo(featureSetOf(3, 4, 5)));
        assertTrue(featureSetOf(1).isUpgradableTo(featureSetOf(1)));
        assertTrue(featureSetOf(2, 3, 4).isUpgradableTo(featureSetOf(2, 3, 4)));
        assertTrue(featureSetOf(2, 3, 4).isUpgradableTo(featureSetOf(2, 3, 4, 5, 6)));
        assertTrue(featureSetOf(2, 3, 4).isUpgradableTo(featureSetOf(2, 3, 4, 5, 10)));
        assertTrue(featureSetOf(0, 10).isUpgradableTo(featureSetOf(0, 1, 2, 10)));
        assertTrue(featureSetOf(2, 3, 4, 8, 10).isUpgradableTo(featureSetOf(2, 3, 4, 5, 6, 8, 10)));
        assertTrue(featureSetOf(2, 3, 4, 8, 10).isUpgradableTo(featureSetOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 10)));

        assertFalse(featureSetOf(1).isUpgradableTo(featureSetOf(0)));
        assertFalse(featureSetOf(0, 1, 2).isUpgradableTo(featureSetOf(4, 5, 6)));
        assertFalse(featureSetOf(0, 1, 2, 3).isUpgradableTo(featureSetOf(1, 2)));
        assertFalse(featureSetOf(2, 3, 4).isUpgradableTo(featureSetOf(0, 1)));
        assertFalse(featureSetOf(2, 3, 4, 10).isUpgradableTo(featureSetOf(2, 3, 4, 5, 6, 11)));
        assertFalse(featureSetOf(2, 3, 4, 8, 10).isUpgradableTo(featureSetOf(2, 3, 4, 5, 6, 10)));
    }

    /** */
    @Test
    public void testDifference() {
        assertTrue(featureSetOf(0).difference(featureSetOf(0)).isEmpty());
        assertTrue(featureSetOf(0, 1, 2).difference(featureSetOf(0, 1, 2)).isEmpty());
        assertTrue(featureSetOf(0, 1, 2, 10).difference(featureSetOf(0, 1, 2, 10)).isEmpty());
        assertTrue(featureSetOf(2).difference(featureSetOf(2)).isEmpty());
        assertTrue(featureSetOf(2, 3, 4).difference(featureSetOf(2, 3, 4)).isEmpty());
        assertTrue(featureSetOf(2, 3, 4, 10).difference(featureSetOf(2, 3, 4, 10)).isEmpty());

        assertEquals(intList(3), featureSetOf(0, 1, 2).difference(featureSetOf(0, 1, 2, 3)));

        assertEquals(intList(3, 4), featureSetOf(0, 1, 2, 10).difference(featureSetOf(0, 1, 2, 3, 4, 10)));

        assertEquals(intList(6, 7, 8), featureSetOf(2, 3, 4, 5).difference(featureSetOf(2, 3, 4, 5, 6, 7, 8)));

        assertEquals(intList(6, 7, 8, 15), featureSetOf(2, 3, 4, 5, 10).difference(featureSetOf(2, 3, 4, 5, 6, 7, 8, 10, 15)));
    }

    /** */
    @Test
    public void testEqualsIdenticalSets() {
        assertTrue(isIdentical(featureSetOf(0, 1, 2), featureSetOf(0, 1, 2)));
        assertTrue(isIdentical(featureSetOf(0, 1, 2, 10), featureSetOf(0, 1, 2, 10)));
        assertTrue(isIdentical(featureSetOf(1, 2, 3), featureSetOf(1, 2, 3)));
        assertTrue(isIdentical(featureSetOf(1, 2, 3, 10), featureSetOf(1, 2, 3, 10)));

        assertFalse(isIdentical(featureSetOf(0, 1, 2), null));
        assertFalse(isIdentical(featureSetOf(0, 1, 2), featureSetOf(0, 1, 2, 3)));
        assertFalse(isIdentical(featureSetOf(1, 2, 3), featureSetOf(1, 2, 3, 10)));
        assertFalse(isIdentical(featureSetOf(0, 1, 2, 10), featureSetOf(0, 1, 2, 8, 10)));
    }

    /** */
    @Test
    public void testToString() {
        assertEquals("IgniteFeatureSet [2 -> 5]", featureSetOf(2, 3, 4, 5).toString());
        assertEquals("IgniteFeatureSet [2, 8, 10]", featureSetOf(2, 8, 10).toString());
        assertEquals("IgniteFeatureSet [2, 3, 8, 10]", featureSetOf(2, 3, 8, 10).toString());
        assertEquals("IgniteFeatureSet [2 -> 5, 8, 10]", featureSetOf(2, 3, 4, 5, 8, 10).toString());
    }

    /** */
    @Test
    public void testBuildInput() {
        assertThrows(log, () -> IgniteFeatureSet.buildFrom(Collections.emptyList()), IllegalArgumentException.class, null);
        assertThrows(log, () -> IgniteFeatureSet.buildFrom((Class<?>)null), NullPointerException.class, null);
        assertThrows(log, () -> IgniteFeatureSet.buildFrom((List<IgniteFeature>)null), NullPointerException.class, null);
    }

    /** */
    private IgniteFeatureSet featureSetOf(int... ids) {
        List<IgniteFeature> features = new ArrayList<>();

        for (int id : ids)
            features.add(new IgniteCoreFeature(id));

        return IgniteFeatureSet.buildFrom(features);
    }

    /** */
    private void checkFeatureSetElements(List<Integer> expElements, IgniteFeatureSet featureSet) {
        List<Integer> curElements = new ArrayList<>();

        Iterator<Integer> iter = featureSet.iterator();

        while (iter.hasNext())
            curElements.add(iter.next());

        assertThrows(log, iter::next, NoSuchElementException.class, null);

        assertEquals(expElements, curElements);
        assertTrue(expElements.stream().allMatch(featureSet::contains));
    }

    /** */
    private boolean isIdentical(IgniteFeatureSet lhs, IgniteFeatureSet rhs) {
        return lhs.equals(rhs) && lhs.hashCode() == rhs.hashCode();
    }

    /** */
    private static GridIntList intList(int... vals) {
        return new GridIntList(vals);
    }
}
