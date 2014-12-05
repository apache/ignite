/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.product.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 * Tests version methods.
 */
public class GridVersionSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testVersions() throws Exception {
        String propVal = System.getProperty(GG_UPDATE_NOTIFIER);

        System.setProperty(GG_UPDATE_NOTIFIER, "true");

        try {
            Ignite ignite = startGrid();

            IgniteProductVersion currVer = ignite.product().version();

            String newVer = null;

            for (int i = 0; i < 30; i++) {
                newVer = ignite.product().latestVersion();

                if (newVer != null)
                    break;

                U.sleep(100);
            }

            info("Versions [cur=" + currVer + ", latest=" + newVer + ']');

            assertNotNull(newVer);
            assertNotSame(currVer.toString(), newVer);
        }
        finally {
            stopGrid();

            if (propVal != null)
                System.setProperty(GG_UPDATE_NOTIFIER, propVal);
            else
                System.clearProperty(GG_UPDATE_NOTIFIER);
        }
    }
}
