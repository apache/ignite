/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Tests external process execution.
 */
public class GridHadoopExternalTaskExecutorSelfTest extends GridHadoopAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testStartProcess() throws Exception {
        try {
            startGrids(1);

            GridHadoopProcessorAdapter hadoop = ((GridKernal)grid(0)).context().hadoop();

        }
        finally {
            stopAllGrids();
        }
    }
}
