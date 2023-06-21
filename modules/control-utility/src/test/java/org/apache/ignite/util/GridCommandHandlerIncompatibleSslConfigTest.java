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

package org.apache.ignite.util;

import java.util.List;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 *
 */
public class GridCommandHandlerIncompatibleSslConfigTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** {@inheritDoc} */
    @Override protected boolean sslEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void extendSslParams(List<String> params) {
        params.add("--ssl-factory");  // incompatible
        params.add("src/test/resources/some-file.xml");
        params.add("--keystore");     // incompatible
        params.add("src/test/resources/some-file.jks");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Assume.assumeTrue(invoker.equalsIgnoreCase(CLI_INVOKER));

        super.beforeTest();
    }

    /** */
    @Test
    public void test() throws Exception {
        startGrids(1);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--set-state", "ACTIVE"));

        assertContains(log, testOut.toString(), "Incorrect SSL configuration.");
    }
}
