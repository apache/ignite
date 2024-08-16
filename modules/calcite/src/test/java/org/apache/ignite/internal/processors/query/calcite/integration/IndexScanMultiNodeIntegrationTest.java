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
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Index scan test.
 */
public class IndexScanMultiNodeIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Test
    public void testComplexKeyScan() {
        IgniteCache<EmployerKey, Employer> emp = client.getOrCreateCache(
            new CacheConfiguration<EmployerKey, Employer>("emp")
                .setSqlSchema("PUBLIC")
                .setQueryEntities(F.asList(new QueryEntity(EmployerKey.class, Employer.class).setTableName("emp")))
        );

        for (int i = 0; i < 100; i++)
            emp.put(new EmployerKey(i, i), new Employer("emp" + i, (double)i));

        assertQuery("SELECT /*+ FORCE_INDEX(\"_key_PK\") */ _key FROM emp").resultSize(100).check();
    }

    /** */
    @Test
    public void testCompoundObjectComparison() {
        IgniteCache<Integer, Employer> emp = client.getOrCreateCache(
            new CacheConfiguration<Integer, Employer>("emp")
                .setSqlSchema("PUBLIC")
                .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp")))
        );

        emp.put(0, new Employer("emp", 0d));

        assertQuery("SELECT _key FROM emp WHERE _val = ?").withParams(new Employer("emp", 0d)).resultSize(1).check();
    }

    /** */
    private static class EmployerKey {
        /** */
        @QuerySqlField
        private final int id0;

        /** */
        @QuerySqlField
        private final int id1;

        /** */
        private EmployerKey(int id0, int id1) {
            this.id0 = id0;
            this.id1 = id1;
        }
    }
}
