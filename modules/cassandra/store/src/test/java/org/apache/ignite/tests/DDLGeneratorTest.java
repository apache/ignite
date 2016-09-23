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
    private static final String URL1 = "org/apache/ignite/tests/persistence/primitive/persistence-settings-1.xml";
    private static final String URL2 = "org/apache/ignite/tests/persistence/pojo/persistence-settings-3.xml";
    private static final String URL3 = "org/apache/ignite/tests/persistence/pojo/persistence-settings-4.xml";

    @Test
    @SuppressWarnings("unchecked")
    /** */
    public void generatorTest() {
        ClassLoader clsLdr = DDLGeneratorTest.class.getClassLoader();

        URL url1 = clsLdr.getResource(URL1);
        if (url1 == null)
            throw new IllegalStateException("Failed to find resource: " + URL1);

        URL url2 = clsLdr.getResource(URL2);
        if (url2 == null)
            throw new IllegalStateException("Failed to find resource: " + URL2);

        URL url3 = clsLdr.getResource(URL3);
        if (url3 == null)
            throw new IllegalStateException("Failed to find resource: " + URL3);

        String file1 = url1.getFile();
        String file2 = url2.getFile();
        String file3 = url3.getFile();

        DDLGenerator.main(new String[]{file1, file2, file3});
    }

}
