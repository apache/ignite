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
        "org/apache/ignite/tests/persistence/pojo/product.xml",
        "org/apache/ignite/tests/persistence/pojo/order.xml"
    };

    /**
     * Test DDL generator.
     */
    @Test
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
