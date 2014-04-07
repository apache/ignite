/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang.utils;

import org.gridgain.testframework.junits.common.*;
import org.pcollections.*;

/**
 *
 */
public class GridPCollectionsTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testPvector() {
        PVector<Integer> vector = TreePVector.empty();

        for (int i = 0; i < 10; i++)
            vector = vector.plus(i);

        assert vector.size() == 10;

        for (int i = 0; i < 10; i++)
            assert vector.contains(i);

        for (int i = 0; i < 5; i++)
            vector = vector.minus(new Integer(i));

        assert vector.size() == 5;

        for (int i = 0; i < 10; i++)
            assert !vector.contains(i) || i >= 5;

        for (int i = 0; i < 10; i++)
            vector = vector.minus(new Integer(i));

        assert vector.isEmpty();
    }
}
