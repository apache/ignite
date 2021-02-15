/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import org.apache.ignite.IgniteClientSpringBean;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/** Tests {@link IgniteClientSpringBean} declaration and configuration. */
public class IgniteClientSpringBeanTest extends GridCommonAbstractTest {
    /** Tests {@link IgniteClientSpringBean} declaration through Spring XML configuration. */
    @Test
    public void testXmlConfiguration() {
        try (
            AbstractApplicationContext ctx = new ClassPathXmlApplicationContext(
                "org/apache/ignite/internal/ignite-client-spring-bean.xml")
        ) {
            IgniteClient cli = ctx.getBean(IgniteClient.class);

            assertNotNull(cli);
            assertEquals(1, cli.cluster().nodes().size());
        }
    }

    /** Tests {@link IgniteClientSpringBean} behaviour in case {@link ClientConfiguration} is not specified. */
    @Test
    public void testOmittedClientConfigurationFailure() {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                IgniteClientSpringBean igniteCliSpringBean = new IgniteClientSpringBean();

                igniteCliSpringBean.start();

                return null;
            },
            IllegalArgumentException.class,
            "Ignite client configuration must be set."
        );
    }
}
