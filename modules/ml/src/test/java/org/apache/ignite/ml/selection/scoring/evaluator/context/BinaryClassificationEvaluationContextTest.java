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

package org.apache.ignite.ml.selection.scoring.evaluator.context;

import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link BinaryClassificationEvaluationContext} class.
 */
public class BinaryClassificationEvaluationContextTest {
    /**
     *
     */
    @Test
    public void testAggregate() {
        BinaryClassificationEvaluationContext<Double> ctx = new BinaryClassificationEvaluationContext<>();
        ctx.aggregate(VectorUtils.of().labeled(1.0));
        assertEquals(ctx.getFirstClsLbl(), 1., 0.);
        assertEquals(ctx.getSecondClsLbl(), null);

        ctx.aggregate(VectorUtils.of().labeled(0.0));
        assertEquals(ctx.getFirstClsLbl(), 0., 0.);
        assertEquals(ctx.getSecondClsLbl(), 1., 0.);
    }

    /**
     *
     */
    @Test(expected = IllegalArgumentException.class)
    public void testAggregateWithThreeLabels() {
        BinaryClassificationEvaluationContext<Double> ctx = new BinaryClassificationEvaluationContext<>();
        ctx.aggregate(VectorUtils.of().labeled(-1.0));
        ctx.aggregate(VectorUtils.of().labeled(1.0));

        assertEquals(ctx.getFirstClsLbl(), -1., 0.);
        assertEquals(ctx.getSecondClsLbl(), 1., 0.);

        ctx.aggregate(VectorUtils.of().labeled(0.0));
    }

    /**
     *
     */
    @Test
    public void testMerge1() {
        BinaryClassificationEvaluationContext<Double> left = new BinaryClassificationEvaluationContext<>();
        BinaryClassificationEvaluationContext<Double> right = new BinaryClassificationEvaluationContext<>();

        BinaryClassificationEvaluationContext<Double> res = left.mergeWith(right);
        assertEquals(res.getFirstClsLbl(), null);
        assertEquals(res.getSecondClsLbl(), null);
    }

    /**
     *
     */
    @Test
    public void testMerge2() {
        BinaryClassificationEvaluationContext<Double> left = new BinaryClassificationEvaluationContext<>(0., null);
        BinaryClassificationEvaluationContext<Double> right = new BinaryClassificationEvaluationContext<>();

        BinaryClassificationEvaluationContext<Double> res = left.mergeWith(right);
        assertEquals(res.getFirstClsLbl(), 0., 0.);
        assertEquals(res.getSecondClsLbl(), null);

        res = right.mergeWith(left);
        assertEquals(res.getFirstClsLbl(), 0., 0.);
        assertEquals(res.getSecondClsLbl(), null);
    }

    /**
     *
     */
    @Test
    public void testMerge3() {
        BinaryClassificationEvaluationContext<Double> left = new BinaryClassificationEvaluationContext<>(null, 0.);
        BinaryClassificationEvaluationContext<Double> right = new BinaryClassificationEvaluationContext<>();

        BinaryClassificationEvaluationContext<Double> res = left.mergeWith(right);
        assertEquals(res.getFirstClsLbl(), 0., 0.);
        assertEquals(res.getSecondClsLbl(), null);

        res = right.mergeWith(left);
        assertEquals(res.getFirstClsLbl(), 0., 0.);
        assertEquals(res.getSecondClsLbl(), null);
    }

    /**
     *
     */
    @Test
    public void testMerge4() {
        BinaryClassificationEvaluationContext<Double> left = new BinaryClassificationEvaluationContext<>(1., 0.);
        BinaryClassificationEvaluationContext<Double> right = new BinaryClassificationEvaluationContext<>();

        BinaryClassificationEvaluationContext<Double> res = left.mergeWith(right);
        assertEquals(res.getFirstClsLbl(), 0., 0.);
        assertEquals(res.getSecondClsLbl(), 1., 0.);

        res = right.mergeWith(left);
        assertEquals(res.getFirstClsLbl(), 0., 0.);
        assertEquals(res.getSecondClsLbl(), 1., 0.);
    }

    /**
     *
     */
    @Test
    public void testMerge5() {
        BinaryClassificationEvaluationContext<Double> left = new BinaryClassificationEvaluationContext<>(1., 0.);
        BinaryClassificationEvaluationContext<Double> right = new BinaryClassificationEvaluationContext<>(0., 1.);

        BinaryClassificationEvaluationContext<Double> res = left.mergeWith(right);
        assertEquals(res.getFirstClsLbl(), 0., 0.);
        assertEquals(res.getSecondClsLbl(), 1., 0.);

        res = right.mergeWith(left);
        assertEquals(res.getFirstClsLbl(), 0., 0.);
        assertEquals(res.getSecondClsLbl(), 1., 0.);
    }

    /**
     *
     */
    @Test(expected = IllegalArgumentException.class)
    public void testMerge6() {
        BinaryClassificationEvaluationContext<Double> left = new BinaryClassificationEvaluationContext<>(1., 0.);
        BinaryClassificationEvaluationContext<Double> right = new BinaryClassificationEvaluationContext<>(2., 1.);
        BinaryClassificationEvaluationContext<Double> res = left.mergeWith(right);
    }
}
