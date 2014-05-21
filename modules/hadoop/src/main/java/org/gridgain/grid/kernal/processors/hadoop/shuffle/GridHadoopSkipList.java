/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.util.*;
import org.gridgain.grid.util.offheap.unsafe.*;

import java.util.*;

/**
 * Skip list.
 */
public class GridHadoopSkipList {
    /** */
    private final Comparator cmp;

    /** */
    private final GridUnsafeMemory mem;

    public GridHadoopSkipList(Comparator cmp, GridUnsafeMemory mem) {
        assert cmp != null;
        assert mem != null;

        this.cmp = cmp;
        this.mem = mem;
    }

    public class ListAdder {
        /** */
        private Random rnd = new GridRandom();

        /** */
        private Object tmpKey;

    }

    /**
     * @param rnd Random.
     * @return Next level.
     */
    static int nextLevel(Random rnd) {
        int x = rnd.nextInt();

        int level = 0;

        while ((x & 1) != 0) { // Count sequential 1 bits.
            level++;

            x >>>= 1;
        }

        return level;
    }
}
