/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.*;

/**
 * TODO: Add class description.
 */
public class GridHadoopExternalRegistrySelfTest extends GridHadoopAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testInitialize() throws Exception {
        try {
            startGrids(1);

            GridHadoopOpProcessor hadoop = (GridHadoopOpProcessor)((GridKernal)grid(0)).context().hadoop();

            GridHadoopExternalProcessRegistry registry = new GridHadoopExternalProcessRegistry(hadoop.context());

            registry.start();
        }
        finally {
            stopAllGrids();
        }
    }
}
