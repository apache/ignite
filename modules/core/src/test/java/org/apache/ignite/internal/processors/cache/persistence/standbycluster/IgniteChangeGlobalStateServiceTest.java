/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence.standbycluster;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteChangeGlobalStateServiceTest extends IgniteChangeGlobalStateAbstractTest {
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
    @Test
    public void testDeployService() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-6629");

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
