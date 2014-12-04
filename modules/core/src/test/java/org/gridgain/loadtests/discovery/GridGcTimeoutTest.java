/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.discovery;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.dataload.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 *
 */
public class GridGcTimeoutTest {
    /** */
    public static final String CFG_PATH = "modules/core/src/test/config/discovery-stress.xml";

    /** */
    public static final int VALUE_SIZE = 1024;

    /**
     * @param args Args.
     * @throws GridException If failed.
     */
    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) throws GridException {
        Ignite g = G.start(U.resolveGridGainUrl(CFG_PATH));

        GridDataLoader<Long, String> ldr = g.dataLoader(null);

        ldr.perNodeBufferSize(16 * 1024);

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < VALUE_SIZE - 42; i++)
            sb.append('a');

        String str = sb.toString();
        long cntr = 0;

        while (true) {
            ldr.addData(cntr++, UUID.randomUUID() + str);

            if (cntr % 1000000 == 0)
                X.println("!!! Entries added: " + cntr);
        }
    }
}
