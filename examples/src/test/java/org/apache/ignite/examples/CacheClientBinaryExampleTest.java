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

package org.apache.ignite.examples;

import org.apache.ignite.examples.binary.datagrid.CacheClientBinaryPutGetExample;
import org.apache.ignite.examples.binary.datagrid.CacheClientBinaryQueryExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;

/**
 *
 */
public class CacheClientBinaryExampleTest extends GridAbstractExamplesTest {
    /** {@inheritDoc} */
    @Override protected String defaultConfig() {
        return "examples/config/binary/example-ignite-binary.xml";
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryPutGetExample() throws Exception {
        CacheClientBinaryPutGetExample.main(new String[] {});
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryQueryExample() throws Exception {
        CacheClientBinaryQueryExample.main(new String[] {});
    }
}
