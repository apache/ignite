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

import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.GiniSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SplitInfo;
import org.junit.Test;

import java.util.stream.DoubleStream;

/**
 * Test of {@link GiniSplitCalculator}.
 */
public class GiniSplitCalculatorTest {
    /** Test calculation of region info consisting from one point. */
    @Test
    public void testCalculateRegionInfoSimple() {
        double labels[] = new double[] {0.0};

        assert new GiniSplitCalculator(labels).calculateRegionInfo(DoubleStream.of(labels), 0).impurity() == 0.0;
    }

    /** Test calculation of region info consisting from two distinct classes. */
    @Test
    public void testCalculateRegionInfoTwoClasses() {
        double labels[] = new double[] {0.0, 1.0};

        assert new GiniSplitCalculator(labels).calculateRegionInfo(DoubleStream.of(labels), 0).impurity() == 0.5;
    }

    /** Test calculation of region info consisting from three distinct classes. */
    @Test
    public void testCalculateRegionInfoThreeClasses() {
        double labels[] = new double[] {0.0, 1.0, 2.0};

        assert Math.abs(new GiniSplitCalculator(labels).calculateRegionInfo(DoubleStream.of(labels), 0).impurity() - 2.0 / 3) < 1E-5;
    }

    /** Test calculation of split of region consisting from one point. */
    @Test
    public void testSplitSimple() {
        double labels[] = new double[] {0.0};
        double values[] = new double[] {0.0};
        Integer[] samples = new Integer[] {0};

        int cnts[] = new int[] {1};

        GiniSplitCalculator.GiniData data = new GiniSplitCalculator.GiniData(0.0, 1, cnts, 1);

        assert new GiniSplitCalculator(labels).splitRegion(samples, values, labels, 0, data) == null;
    }

    /** Test calculation of split of region consisting from two points. */
    @Test
    public void testSplitTwoClassesTwoPoints() {
        double labels[] = new double[] {0.0, 1.0};
        double values[] = new double[] {0.0, 1.0};
        Integer[] samples = new Integer[] {0, 1};

        int cnts[] = new int[] {1, 1};

        GiniSplitCalculator.GiniData data = new GiniSplitCalculator.GiniData(0.5, 2, cnts, 1.0 * 1.0 + 1.0 * 1.0);

        SplitInfo<GiniSplitCalculator.GiniData> split = new GiniSplitCalculator(labels).splitRegion(samples, values, labels, 0, data);

        assert split.leftData().impurity() == 0;
        assert split.leftData().counts()[0] == 1;
        assert split.leftData().counts()[1] == 0;
        assert split.leftData().getSize() == 1;

        assert split.rightData().impurity() == 0;
        assert split.rightData().counts()[0] == 0;
        assert split.rightData().counts()[1] == 1;
        assert split.rightData().getSize() == 1;
    }

    /** Test calculation of split of region consisting from four distinct values. */
    @Test
    public void testSplitTwoClassesFourPoints() {
        double labels[] = new double[] {0.0, 0.0, 1.0, 1.0};
        double values[] = new double[] {0.0, 1.0, 2.0, 3.0};

        Integer[] samples = new Integer[] {0, 1, 2, 3};

        int[] cnts = new int[] {2, 2};

        GiniSplitCalculator.GiniData data = new GiniSplitCalculator.GiniData(0.5, 4, cnts, 2.0 * 2.0 + 2.0 * 2.0);

        SplitInfo<GiniSplitCalculator.GiniData> split = new GiniSplitCalculator(labels).splitRegion(samples, values, labels, 0, data);

        assert split.leftData().impurity() == 0;
        assert split.leftData().counts()[0] == 2;
        assert split.leftData().counts()[1] == 0;
        assert split.leftData().getSize() == 2;

        assert split.rightData().impurity() == 0;
        assert split.rightData().counts()[0] == 0;
        assert split.rightData().counts()[1] == 2;
        assert split.rightData().getSize() == 2;
    }

    /** Test calculation of split of region consisting from three distinct values. */
    @Test
    public void testSplitThreePoints() {
        double labels[] = new double[] {0.0, 1.0, 2.0};
        double values[] = new double[] {0.0, 1.0, 2.0};
        Integer[] samples = new Integer[] {0, 1, 2};

        int[] cnts = new int[] {1, 1, 1};

        GiniSplitCalculator.GiniData data = new GiniSplitCalculator.GiniData(2.0 / 3, 3, cnts, 1.0 * 1.0 + 1.0 * 1.0 + 1.0 * 1.0);

        SplitInfo<GiniSplitCalculator.GiniData> split = new GiniSplitCalculator(labels).splitRegion(samples, values, labels, 0, data);

        assert split.leftData().impurity() == 0.0;
        assert split.leftData().counts()[0] == 1;
        assert split.leftData().counts()[1] == 0;
        assert split.leftData().counts()[2] == 0;
        assert split.leftData().getSize() == 1;

        assert split.rightData().impurity() == 0.5;
        assert split.rightData().counts()[0] == 0;
        assert split.rightData().counts()[1] == 1;
        assert split.rightData().counts()[2] == 1;
        assert split.rightData().getSize() == 2;
    }
}
