package org.apache.ignite.spi.deployment.uri.scanners.http;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.*;
import org.eclipse.jetty.util.resource.*;
import org.apache.ignite.spi.deployment.uri.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;

import javax.servlet.http.*;
import java.util.*;

import static org.eclipse.jetty.http.HttpHeader.*;

/**
 * Test http scanner.
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
public class GridHttpDeploymentSelfTest extends GridUriDeploymentAbstractSelfTest {
    /** Jetty. */
    private Server srv;

    /** {@inheritDoc} */
    @Override protected void beforeSpiStarted() throws Exception {
        srv = new Server();

        ServerConnector conn = new ServerConnector(srv);

        conn.setPort(8080);

        srv.addConnector(conn);

        ResourceHandler hnd = new ResourceHandler() {
            @Override protected void doResponseHeaders(HttpServletResponse resp, Resource res, String mimeTyp) {
                super.doResponseHeaders(resp, res, mimeTyp);

                resp.setDateHeader(LAST_MODIFIED.asString(), res.lastModified());
            }
        };

        hnd.setDirectoriesListed(true);
        hnd.setResourceBase(
            U.resolveGridGainPath(GridTestProperties.getProperty("ant.urideployment.gar.path")).getPath());

        srv.setHandler(hnd);

        srv.start();

        assert srv.isStarted();
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTestsStopped() throws Exception {
        assert srv.isStarted();

        srv.stop();

        assert srv.isStopped();
    }

    /**
     * @throws Exception if failed.
     */
    public void testDeployment() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask3");
        checkTask("GridUriDeploymentTestWithNameTask3");
    }

    /**
     * @return Test server URl as deployment source URI.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        return Collections.singletonList("http://freq=5000@localhost:8080/");
    }
}
