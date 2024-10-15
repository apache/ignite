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

package org.apache.ignite.internal.processors.query.calcite.integration;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Integration test for user defined functions.
 */
public class UserDefinedFunctionsIntegrationTransactionalTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    @Test
    public void testFunctions() {
        // Cache with PUBLIC schema.
        IgniteCache<Integer, Employer> emp3 = client.getOrCreateCache(this.<Integer, Employer>cacheConfiguration()
            .setName("emp3")
            .setSqlFunctionClasses(UserDefinedFunctionsIntegrationTest.OtherFunctionsLibrary.class)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp3")))
        );

        put(client, emp3, 1, new Employer("Igor3", 100d));
        put(client, emp3, 2, new Employer("Roman3", 200d));

        assertQuery("SELECT sq(salary) FROM emp3").returns(10_000d).returns(40_000d).check();
        assertQuery("SELECT echo(name) FROM emp3").returns("Igor3").returns("Roman3").check();
    }
}
