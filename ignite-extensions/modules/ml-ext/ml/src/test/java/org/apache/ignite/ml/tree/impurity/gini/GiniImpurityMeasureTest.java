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

package org.apache.ignite.ml.tree.impurity.gini;

import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link GiniImpurityMeasure}.
 */
public class GiniImpurityMeasureTest {
    /** */
    @Test
    public void testImpurityOnEmptyData() {
        long[] left = new long[]{0, 0, 0};
        long[] right = new long[]{0, 0, 0};

        GiniImpurityMeasure impurity = new GiniImpurityMeasure(left, right);

        assertEquals(0.0, impurity.impurity(), 1e-10);
    }

    /** */
    @Test
    public void testImpurityLeftPart() {
        long[] left = new long[]{3, 0, 0};
        long[] right = new long[]{0, 0, 0};

        GiniImpurityMeasure impurity = new GiniImpurityMeasure(left, right);

        assertEquals(-3, impurity.impurity(), 1e-10);
    }

    /** */
    @Test
    public void testImpurityRightPart() {
        long[] left = new long[]{0, 0, 0};
        long[] right = new long[]{3, 0, 0};

        GiniImpurityMeasure impurity = new GiniImpurityMeasure(left, right);

        assertEquals(-3, impurity.impurity(), 1e-10);
    }

    /** */
    @Test
    public void testImpurityLeftAndRightPart() {
        long[] left = new long[]{3, 0, 0};
        long[] right = new long[]{0, 3, 0};

        GiniImpurityMeasure impurity = new GiniImpurityMeasure(left, right);

        assertEquals(-6, impurity.impurity(), 1e-10);
    }

    /** */
    @Test
    public void testAdd() {
        Random rnd = new Random(0);

        GiniImpurityMeasure a = new GiniImpurityMeasure(
            new long[]{randCnt(rnd), randCnt(rnd), randCnt(rnd)},
            new long[]{randCnt(rnd), randCnt(rnd), randCnt(rnd)}
        );

        GiniImpurityMeasure b = new GiniImpurityMeasure(
            new long[]{randCnt(rnd), randCnt(rnd), randCnt(rnd)},
            new long[]{randCnt(rnd), randCnt(rnd), randCnt(rnd)}
        );

        GiniImpurityMeasure c = a.add(b);

        assertEquals(a.getLeft()[0] + b.getLeft()[0], c.getLeft()[0]);
        assertEquals(a.getLeft()[1] + b.getLeft()[1], c.getLeft()[1]);
        assertEquals(a.getLeft()[2] + b.getLeft()[2], c.getLeft()[2]);

        assertEquals(a.getRight()[0] + b.getRight()[0], c.getRight()[0]);
        assertEquals(a.getRight()[1] + b.getRight()[1], c.getRight()[1]);
        assertEquals(a.getRight()[2] + b.getRight()[2], c.getRight()[2]);
    }

    /** */
    @Test
    public void testSubtract() {
        Random rnd = new Random(0);

        GiniImpurityMeasure a = new GiniImpurityMeasure(
            new long[]{randCnt(rnd), randCnt(rnd), randCnt(rnd)},
            new long[]{randCnt(rnd), randCnt(rnd), randCnt(rnd)}
        );

        GiniImpurityMeasure b = new GiniImpurityMeasure(
            new long[]{randCnt(rnd), randCnt(rnd), randCnt(rnd)},
            new long[]{randCnt(rnd), randCnt(rnd), randCnt(rnd)}
        );

        GiniImpurityMeasure c = a.subtract(b);

        assertEquals(a.getLeft()[0] - b.getLeft()[0], c.getLeft()[0]);
        assertEquals(a.getLeft()[1] - b.getLeft()[1], c.getLeft()[1]);
        assertEquals(a.getLeft()[2] - b.getLeft()[2], c.getLeft()[2]);

        assertEquals(a.getRight()[0] - b.getRight()[0], c.getRight()[0]);
        assertEquals(a.getRight()[1] - b.getRight()[1], c.getRight()[1]);
        assertEquals(a.getRight()[2] - b.getRight()[2], c.getRight()[2]);
    }

    /** Generates random count. */
    private long randCnt(Random rnd) {
        return Math.abs(rnd.nextInt());
    }
}
