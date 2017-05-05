package org.apache.ignite.cache.database.standbycluster;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDescriptor;

/**
 *
 */
public class GridChangeGlobalStateServiceTest extends GridChangeGlobalStateAbstractTest {
    /** {@inheritDoc} */
    @Override protected int backUpClientNodes() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected int primaryNodes() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int backUpNodes() {
        return 1;
    }

    /**
     *
     */
    public void testDeployService() throws Exception {
        Ignite ig1P = primary(0);

        Ignite ig1B = backUp(0);

        String serName = "service";

        ServiceConfiguration serConf = new ServiceConfiguration();
        serConf.setTotalCount(1);
        serConf.setName(serName);
        serConf.setService(new TestService());

        ig1P.services().deploy(serConf);

        stopAllPrimary();

        ig1B.active(true);

        U.sleep(3000);

        Collection<ServiceDescriptor> descs = ig1B.services().serviceDescriptors();

        assertTrue(!F.isEmpty(descs));

        TestService srv = ig1B.services().service(serName);

        assertTrue(srv != null);
    }

    /**
     *
     */
    private static class TestService implements Service{
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            System.out.println("cancel service");
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            System.out.println("init service");
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            System.out.println("execute service");
        }
    }
}
