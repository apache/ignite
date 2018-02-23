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

package org.apache.ignite.ml.trees;

import java.util.stream.DoubleStream;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.VarianceSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SplitInfo;
import org.junit.Test;

/**
 * Test for {@link VarianceSplitCalculator}.
 */
public class VarianceSplitCalculatorTest {
    /** Test calculation of region info consisting from one point. */
    @Test
    public void testCalculateRegionInfoSimple() {
        double labels[] = new double[] {0.0};

        assert new VarianceSplitCalculator().calculateRegionInfo(DoubleStream.of(labels), 1).impurity() == 0.0;
    }

    /** Test calculation of region info consisting from two classes. */
    @Test
    public void testCalculateRegionInfoTwoClasses() {
        double labels[] = new double[] {0.0, 1.0};

        assert new VarianceSplitCalculator().calculateRegionInfo(DoubleStream.of(labels), 2).impurity() == 0.25;
    }

    /** Test calculation of region info consisting from three classes. */
    @Test
    public void testCalculateRegionInfoThreeClasses() {
        double labels[] = new double[] {1.0, 2.0, 3.0};

        assert Math.abs(new VarianceSplitCalculator().calculateRegionInfo(DoubleStream.of(labels), 3).impurity() - 2.0 / 3) < 1E-10;
    }

    /** Test calculation of split of region consisting from one point. */
    @Test
    public void testSplitSimple() {
        double labels[] = new double[] {0.0};
        double values[] = new double[] {0.0};
        Integer[] samples = new Integer[] {0};

        VarianceSplitCalculator.VarianceData data = new VarianceSplitCalculator.VarianceData(0.0, 1, 0.0);

        assert new VarianceSplitCalculator().splitRegion(samples, values, labels, 0, data) == null;
    }

    /** Test calculation of split of region consisting from two classes. */
    @Test
    public void testSplitTwoClassesTwoPoints() {
        double labels[] = new double[] {0.0, 1.0};
        double values[] = new double[] {0.0, 1.0};
        Integer[] samples = new Integer[] {0, 1};

        VarianceSplitCalculator.VarianceData data = new VarianceSplitCalculator.VarianceData(0.25, 2, 0.5);

        SplitInfo<VarianceSplitCalculator.VarianceData> split = new VarianceSplitCalculator().splitRegion(samples, values, labels, 0, data);

        assert split.leftData().impurity() == 0;
        assert split.leftData().mean() == 0;
        assert split.leftData().getSize() == 1;

        assert split.rightData().impurity() == 0;
        assert split.rightData().mean() == 1;
        assert split.rightData().getSize() == 1;
    }
}
