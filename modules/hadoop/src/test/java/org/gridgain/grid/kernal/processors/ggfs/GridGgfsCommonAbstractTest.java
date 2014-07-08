/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Common subclass for all GGFS tests. Aimed to disabled peer class loading which is restricted for Hadoop edition.
 */
public class GridGgfsCommonAbstractTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration() throws Exception {
        GridConfiguration cfg = super.getConfiguration();

        cfg.setPeerClassLoadingEnabled(false);
        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);
        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(GridTestResources rsrcs) throws Exception {
        GridConfiguration cfg = super.getConfiguration(rsrcs);

        cfg.setPeerClassLoadingEnabled(false);
        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName, GridTestResources rsrcs) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName, rsrcs);

        cfg.setPeerClassLoadingEnabled(false);
        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }
}
