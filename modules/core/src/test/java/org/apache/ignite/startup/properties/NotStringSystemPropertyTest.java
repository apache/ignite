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

package org.apache.ignite.startup.properties;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * The test checks start of Ignite with non-string properties.
 */
public class NotStringSystemPropertyTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration optimize(IgniteConfiguration cfg) throws IgniteCheckedException {
        IgniteConfiguration oCfg = super.optimize(cfg);
        oCfg.setIncludeProperties(null);
        return cfg;
    }

    /**
     * @throws Exception If fail.
     */
    public void testGridStart() throws Exception {
        try {
            System.getProperties().put("prop", new Some(0, "prop"));
            startGridsMultiThreaded(2);
            checkTopology(2);
        } finally {
            stopAllGrids();
        }
    }

    /**
     * Some non-string class.
     */
    private static class Some {

        /**
         * Int field.
         */
        private int count = 0;

        /**
         * String field.
         */
        private String name;

        /**
         * @param count Int value.
         * @param name String value.
         */
        public Some(int count, String name) {
            this.count = count;
            this.name = name;
        }

        /**
         * @return Count.
         */
        public int getCount() {
            return count;
        }

        /**
         * @param count Count.
         */
        public void setCount(int count) {
            this.count = count;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Some{" +
                    "count=" + count +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
