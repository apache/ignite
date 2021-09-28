package org.apache.ignite.internal.processors.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class ServiceExecutionContextTest extends GridCommonAbstractTest {

    private static final String ATTR_NAME = "int.attr";

    private static final String SVC_NAME = "test-svc";

    @Test
    public void testProxyContext() throws Exception {
        startGrids(2);

        TestService svc = new TestServiceImpl();

        grid(0).services().deployClusterSingleton(SVC_NAME, svc);

        checkProxyAttribute(grid(0), false, 1);
        checkProxyAttribute(grid(1), false, 2);
        checkProxyAttribute(grid(0), true, 3);
        checkProxyAttribute(grid(1), true, 4);

        grid(0).services().cancel(SVC_NAME);

        grid(0).services().deployNodeSingleton(SVC_NAME, svc);

        checkProxyAttribute(grid(0), false, 1);
        checkProxyAttribute(grid(1), false, 2);
        checkProxyAttribute(grid(0), true, 3);
        checkProxyAttribute(grid(1), true, 4);
    }

    private void checkProxyAttribute(Ignite node, boolean sticky, Object attrVal) {
        TestService svcProxy = node.services().serviceProxy(SVC_NAME, TestService.class, sticky, F.asMap(ATTR_NAME, attrVal), 0);
        assertEquals(attrVal, svcProxy.attribute());
    }

    private static interface TestService extends Service {
        public Object attribute();
    }

    /** */
    private static class TestServiceImpl implements TestService {
        /** */
        private ServiceContextImpl ctx;
//
//        /** */
//        @LoggerResource
//        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            this.ctx = (ServiceContextImpl)ctx;
        }

        @Override public Object attribute() {
            assert ctx != null;

            return ctx.attribute(ATTR_NAME);
        }

        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }
}
