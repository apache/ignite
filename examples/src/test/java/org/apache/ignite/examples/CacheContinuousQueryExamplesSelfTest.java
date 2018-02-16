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

import org.apache.ignite.examples.datagrid.CacheContinuousAsyncQueryExample;
import org.apache.ignite.examples.datagrid.CacheContinuousQueryExample;
import org.apache.ignite.examples.datagrid.CacheContinuousQueryWithTransformerExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;

/**
 */
public class CacheContinuousQueryExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    public void testCacheContinuousAsyncQueryExample() throws Exception {
        CacheContinuousAsyncQueryExample.main(new String[] {});
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheContinuousQueryExample() throws Exception {
        CacheContinuousQueryExample.main(new String[] {});
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheContinuousQueryWithTransformerExample() throws Exception {
        CacheContinuousQueryWithTransformerExample.main(new String[] {});
    }
}
