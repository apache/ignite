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
package org.apache.ignite.internal.processors.rest;


import java.util.Arrays;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.IgniteCommonsSystemProperties.IGNITE_MARSHALLER_BLACKLIST;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.internal.processors.rest.JettyRestProcessorAbstractSelfTest.assertResponseContainsError;

/** */
public class JettyRestProcessorClassFilterTest extends JettyRestProcessorCommonSelfTest {
    /** */
    public static final int RMT_PORT = 8089;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Arrays.asList("-D" + IGNITE_MARSHALLER_BLACKLIST +
            "=" + U.resolveIgnitePath("modules/core/src/test/config/class_list_exploit_included.txt").getPath(),
            "-D" + IGNITE_JETTY_PORT + "=" + RMT_PORT);
    }

    /** {@inheritDoc} */
    @Override protected String restUrl() {
        return "http://" + LOC_HOST + ":" + RMT_PORT + "/ignite?";
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutIncorrectJson() throws Exception {
        // Check forbidden type.
        ForbiddenType forbidden = new ForbiddenType(new Exploit[] {
            new Exploit(1),
            new Exploit(2)
        });

        String json = JSON_MAPPER.writeValueAsString(forbidden);

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_PUT,
            "keyType", "int",
            "key", "5",
            "valueType", ForbiddenType.class.getName(),
            "val", json
        );

        System.out.println("ret = " + ret);

        assertResponseContainsError(ret, "Deserialization of class " + Exploit.class.getName() + " is disallowed.");
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** */
    private static class ForbiddenType {
        /** Data. */
        @JsonProperty
        private Exploit[] data;

        /** */
        ForbiddenType() {
            // No-op.
        }

        /**
         * @param data Data.
         */
        ForbiddenType(Exploit[] data) {
            this.data = data;
        }
    }

    /** */
    private static class Exploit {
        /** Value. */
        @JsonProperty
        private int val = 10;

        /**
         * @param val Value
         */
        Exploit(int val) {
            this.val = val;
        }

        /** */
        Exploit() {
            // No-op.
        }
    }

    /** {@inheritDoc} */
    @Override protected String signature() {
        return null;
    }
}
