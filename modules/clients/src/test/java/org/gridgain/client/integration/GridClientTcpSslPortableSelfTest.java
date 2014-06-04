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

/**
 * Tests TCP binary protocol with client when SSL is enabled.
 */
public class GridClientTcpSslPortableSelfTest extends GridClientTcpSslSelfTest {
    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() {
        GridClientConfiguration ccfg = super.clientConfiguration();

        ccfg.setMarshaller(new GridClientPortableMarshaller(null));

        return ccfg;
    }
}
