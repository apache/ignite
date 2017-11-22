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

package org.apache.ignite.tests;

import java.net.URL;
import org.apache.ignite.cache.store.cassandra.utils.DDLGenerator;
import org.junit.Test;

/**
 * DDLGenerator test.
 */
public class DDLGeneratorTest {
    /** */
    private static final String[] RESOURCES = new String[] {
        "org/apache/ignite/tests/persistence/primitive/persistence-settings-1.xml",
        "org/apache/ignite/tests/persistence/pojo/persistence-settings-1.xml",
        "org/apache/ignite/tests/persistence/pojo/persistence-settings-2.xml",
        "org/apache/ignite/tests/persistence/pojo/persistence-settings-3.xml",
        "org/apache/ignite/tests/persistence/pojo/persistence-settings-4.xml",
        "org/apache/ignite/tests/persistence/pojo/persistence-settings-5.xml",
        "org/apache/ignite/tests/persistence/pojo/persistence-settings-6.xml",
        "org/apache/ignite/tests/persistence/pojo/persistence-settings-7-handler.xml",
        "org/apache/ignite/tests/persistence/pojo/persistence-settings-8-handler.xml",
        "org/apache/ignite/tests/persistence/pojo/persistence-settings-9-handler.xml",
        "org/apache/ignite/tests/persistence/pojo/product.xml",
        "org/apache/ignite/tests/persistence/pojo/order.xml"
    };

    /**
     * Test DDL generator.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void generatorTest() {
        String[] files = new String[RESOURCES.length];

        ClassLoader clsLdr = DDLGeneratorTest.class.getClassLoader();

        for (int i = 0; i < RESOURCES.length; i++) {
            URL url = clsLdr.getResource(RESOURCES[i]);
            if (url == null)
                throw new IllegalStateException("Failed to find resource: " + RESOURCES[i]);

            files[i] = url.getFile();
        }

        DDLGenerator.main(files);
    }
}
