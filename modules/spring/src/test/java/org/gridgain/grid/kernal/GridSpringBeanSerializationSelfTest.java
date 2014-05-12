/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Test for {@link GridSpringBean} serialization.
 */
public class GridSpringBeanSerializationSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Marshaller. */
    private static final GridMarshaller MARSHALLER = new GridOptimizedMarshaller();

    /** Attribute key. */
    private static final String ATTR_KEY = "checkAttr";

    /** Bean. */
    private GridSpringBean bean;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        bean = new GridSpringBean();

        bean.setConfiguration(config());

        bean.afterPropertiesSet();
    }

    /**
     * @return Grid configuration.
     */
    private GridConfiguration config() {
        GridConfiguration cfg = new GridConfiguration();

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setUserAttributes(F.asMap(ATTR_KEY, true));

        cfg.setRestEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        bean.destroy();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        assert bean != null;

        GridSpringBean bean0 = MARSHALLER.unmarshal(MARSHALLER.marshal(bean), null);

        assert bean0 != null;
        assert bean0.grid() != null;
        assert bean0.log() != null;
        assert bean0.localNode() != null;
        assert bean0.localNode().<Boolean>attribute(ATTR_KEY);
    }
}
