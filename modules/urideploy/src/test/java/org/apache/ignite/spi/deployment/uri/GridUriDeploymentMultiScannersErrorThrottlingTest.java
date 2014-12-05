/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.spi.*;

import java.util.*;

/**
 * Tests error and warn messages throttling.
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentMultiScannersErrorThrottlingTest extends GridUriDeploymentAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testThrottling() throws Exception {
        LT.throttleTimeout(11000);

        // Sleep for 1 min.
        Thread.sleep(60 * 1000);
    }

    /**
     * @return URI list.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        List<String> uriList = new ArrayList<>();

        uriList.add("ftp://anonymous:111111;freq=5000@unknown.host:21/pub/gg-test");
        uriList.add("ftp://anonymous:111111;freq=5000@localhost:21/pub/gg-test");

        uriList.add("http://freq=5000@localhost/tasks");
        uriList.add("http://freq=5000@unknownhost.host/tasks");

        return uriList;
    }
}
