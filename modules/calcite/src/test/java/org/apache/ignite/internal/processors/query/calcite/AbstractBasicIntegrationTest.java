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

package org.apache.ignite.internal.processors.query.calcite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.WithSystemProperty;

/**
 *
 */
@WithSystemProperty(key = "calcite.debug", value = "false")
public class AbstractBasicIntegrationTest extends GridCommonCalciteAbstractTest {
    /** */
    private static IgniteEx client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(3);

        client = startClientGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        for (Ignite ign : G.allGrids()) {
            for (String cacheName : ign.cacheNames())
                ign.destroyCache(cacheName);
        }

        cleanQueryPlanCache();
    }

    /** */
    protected QueryChecker assertQuery(String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryEngine getEngine() {
                return Commons.lookupComponent(client.context(), QueryEngine.class);
            }
        };
    }

    /** */
    protected IgniteCache<Integer, Employer> createAndPopulateTable() {
        IgniteCache<Integer, Employer> person = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>()
            .setName("person")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("person")))
            .setBackups(2)
        );

        int idx = 0;
        person.put(idx++, new Employer("Igor", 10d));
        person.put(idx++, new Employer(null, 15d));
        person.put(idx++, new Employer("Ilya", 15d));
        person.put(idx++, new Employer("Roma", 10d));
        person.put(idx++, new Employer("Roma", 10d));

        return person;
    }

    /** */
    public static class Employer {
        /** */
        @QuerySqlField
        public String name;

        /** */
        @QuerySqlField
        public Double salary;

        /** */
        public Employer(String name, Double salary) {
            this.name = name;
            this.salary = salary;
        }
    }
}
