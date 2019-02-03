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

package org.apache.ignite.ml.tree.impurity.mse;

import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MSEImpurityMeasure}.
 */
public class MSEImpurityMeasureTest {
    /** */
    @Test
    public void testImpurityOnEmptyData() {
        MSEImpurityMeasure impurity = new MSEImpurityMeasure(0, 0, 0, 0, 0, 0);

        assertEquals(0.0, impurity.impurity(), 1e-10);
    }

    /** */
    @Test
    public void testImpurityLeftPart() {
        // Test on left part [1, 2, 2, 1, 1, 1].
        MSEImpurityMeasure impurity = new MSEImpurityMeasure(8, 12, 6, 0, 0, 0);

        assertEquals(1.333, impurity.impurity(), 1e-3);
    }

    /** */
    @Test
    public void testImpurityRightPart() {
        // Test on right part [1, 2, 2, 1, 1, 1].
        MSEImpurityMeasure impurity = new MSEImpurityMeasure(0, 0, 0, 8, 12, 6);

        assertEquals(1.333, impurity.impurity(), 1e-3);
    }

    /** */
    @Test
    public void testImpurityLeftAndRightPart() {
        // Test on left part [1, 2, 2] and right part [1, 1, 1].
        MSEImpurityMeasure impurity = new MSEImpurityMeasure(5, 9, 3, 3, 3, 3);

        assertEquals(0.666, impurity.impurity(), 1e-3);
    }

    /** */
    @Test
    public void testAdd() {
        Random rnd = new Random(0);

        MSEImpurityMeasure a = new MSEImpurityMeasure(
            rnd.nextDouble(), rnd.nextDouble(), rnd.nextInt(), rnd.nextDouble(), rnd.nextDouble(), rnd.nextInt()
        );

        MSEImpurityMeasure b = new MSEImpurityMeasure(
            rnd.nextDouble(), rnd.nextDouble(), rnd.nextInt(), rnd.nextDouble(), rnd.nextDouble(), rnd.nextInt()
        );

        MSEImpurityMeasure c = a.add(b);

        assertEquals(a.getLeftY() + b.getLeftY(), c.getLeftY(), 1e-10);
        assertEquals(a.getLeftY2() + b.getLeftY2(), c.getLeftY2(), 1e-10);
        assertEquals(a.getLeftCnt() + b.getLeftCnt(), c.getLeftCnt());
        assertEquals(a.getRightY() + b.getRightY(), c.getRightY(), 1e-10);
        assertEquals(a.getRightY2() + b.getRightY2(), c.getRightY2(), 1e-10);
        assertEquals(a.getRightCnt() + b.getRightCnt(), c.getRightCnt());
    }

    /** */
    @Test
    public void testSubtract() {
        Random rnd = new Random(0);

        MSEImpurityMeasure a = new MSEImpurityMeasure(
            rnd.nextDouble(), rnd.nextDouble(), rnd.nextInt(), rnd.nextDouble(), rnd.nextDouble(), rnd.nextInt()
        );

        MSEImpurityMeasure b = new MSEImpurityMeasure(
            rnd.nextDouble(), rnd.nextDouble(), rnd.nextInt(), rnd.nextDouble(), rnd.nextDouble(), rnd.nextInt()
        );

        MSEImpurityMeasure c = a.subtract(b);

        assertEquals(a.getLeftY() - b.getLeftY(), c.getLeftY(), 1e-10);
        assertEquals(a.getLeftY2() - b.getLeftY2(), c.getLeftY2(), 1e-10);
        assertEquals(a.getLeftCnt() - b.getLeftCnt(), c.getLeftCnt());
        assertEquals(a.getRightY() - b.getRightY(), c.getRightY(), 1e-10);
        assertEquals(a.getRightY2() - b.getRightY2(), c.getRightY2(), 1e-10);
        assertEquals(a.getRightCnt() - b.getRightCnt(), c.getRightCnt());
    }
}
