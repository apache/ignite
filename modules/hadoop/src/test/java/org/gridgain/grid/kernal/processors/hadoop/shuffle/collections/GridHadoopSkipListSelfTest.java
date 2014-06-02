/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle.collections;

import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static java.lang.Math.*;

/**
 * Skip list tests.
 */
public class GridHadoopSkipListSelfTest  extends GridCommonAbstractTest {

    public void testLevel() {
        Random rnd = new GridRandom();

        int[] levelsCnts = new int[17];

        int all = 10000;

        for (int i = 0; i < all; i++) {
            int level = GridHadoopSkipList.randomLevel(rnd);

            levelsCnts[level]++;
        }

        X.println("Distribution: " + Arrays.toString(levelsCnts));

        for (int level = 0; level < levelsCnts.length; level++) {
            int exp = all >>> (level + 1);

            double precission = 0.72 / Math.max(32 >>> level, 1);

            int sigma = max((int)ceil(precission * exp), 5);

            X.println("Level: " + level + " exp: " + exp + " act: " + levelsCnts[level] + " precission: " + precission +
                " sigma: " + sigma);

            assertTrue(abs(exp - levelsCnts[level]) <= sigma);
        }
    }
}
