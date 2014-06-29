/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.integration;

import org.gridgain.client.*;
import org.gridgain.client.marshaller.portable.*;
import org.gridgain.grid.*;

/**
 *
 */
public class GridClientTcpDirectPortableSelfTest extends GridClientTcpDirectSelfTest {
    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridClientConnectionConfiguration clientCfg = new GridClientConnectionConfiguration();

        clientCfg.setMarshaller(new GridClientPortableMarshaller());

        cfg.setClientConnectionConfiguration(clientCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() throws GridClientException {
        GridClientConfiguration ccfg = super.clientConfiguration();

        ccfg.setMarshaller(new GridClientPortableMarshaller());

        return ccfg;
    }
}
