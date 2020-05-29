/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
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
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-6629")
    @Test
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
    private static class TestService implements Service {
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
