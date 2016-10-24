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
    @Test
    @SuppressWarnings("unchecked")
    /** */
    public void generatorTest() {
        ClassLoader clsLdr = DDLGeneratorTest.class.getClassLoader();

        URL url1 = clsLdr.getResource("org/apache/ignite/tests/persistence/primitive/persistence-settings-1.xml");
        String file1 = url1.getFile(); // TODO IGNITE-1371 Possible NPE

        URL url2 = clsLdr.getResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-3.xml");
        String file2 = url2.getFile();  // TODO IGNITE-1371 Possible NPE

        DDLGenerator.main(new String[]{file1, file2});
    }

}
