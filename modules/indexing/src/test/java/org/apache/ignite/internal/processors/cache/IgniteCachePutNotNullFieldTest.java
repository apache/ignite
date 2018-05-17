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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Collections;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteCachePutNotNullFieldTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        jcache(
            grid(0),
            defaultCacheConfiguration()
                .setQueryEntities(
                    Collections.singleton(
                        new QueryEntity(Organization.class.getName(), Address.class.getName())
                            .addQueryField("address", "java.lang.String", "address")
                            .setNotNullFields(Collections.singleton("address"))
                    )
                ),
            "ORG_ADDRESS"
        );
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    public void testPutAllShouldThrowExceptionWhenPassedNullValue() throws Exception {
        GridTestUtils.assertThrowsWithCause(
            () -> {
                jcache(0, "ORG_ADDRESS")
                    .putAll(Collections.singletonMap(new Organization("1"), new Address(null)));

                return null;
            },
            IgniteSQLException.class
        );
    }

    /** */
    private static class Organization implements Serializable {
        /** */
        private final String name;

        /**
         * @param name Name.
         */
        private Organization(String name) {
            this.name = name;
        }
    }

    /** */
    private static class Address implements Serializable {
        /** */
        private final String addr;

        /**
         * @param addr Address.
         */
        private Address(String addr) {
            this.addr = addr;
        }
    }
}
