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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.io.Serializable;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test for distributed queries with node restarts.
 */
public class IgniteCacheQueryAbstractDistributedJoinSelfTest extends AbstractIndexingCommonTest {
    /** */
    protected static final String QRY_0 = "select co._key, count(*) cnt\n" +
        "from \"pe\".Person pe, \"pr\".Product pr, \"co\".Company co, \"pu\".Purchase pu\n" +
        "where pe._key = pu.personId and pu.productId = pr._key and pr.companyId = co._key \n" +
        "group by co._key order by cnt desc, co._key";

    /** */
    protected static final String QRY_0_BROADCAST = "select co._key, count(*) cnt\n" +
        "from \"co\".Company co, \"pr\".Product pr, \"pu\".Purchase pu, \"pe\".Person pe \n" +
        "where pe._key = pu.personId and pu.productId = pr._key and pr.companyId = co._key \n" +
        "group by co._key order by cnt desc, co._key";

    /** */
    protected static final String QRY_1 = "select pr._key, co._key\n" +
        "from \"pr\".Product pr, \"co\".Company co\n" +
        "where pr.companyId = co._key\n" +
        "order by co._key, pr._key ";

    /** */
    protected static final String QRY_1_BROADCAST = "select pr._key, co._key\n" +
        "from \"co\".Company co, \"pr\".Product pr \n" +
        "where pr.companyId = co._key\n" +
        "order by co._key, pr._key ";

    protected static final String QRY_LONG = "select pe.id, co.id, pr._key\n" +
        "from \"pe\".Person pe, \"pr\".Product pr, \"co\".Company co, \"pu\".Purchase pu\n" +
        "where pe._key = pu.personId and pu.productId = pr._key and pr.companyId = co._key \n" +
        "order by pe.id desc";

    /** */
    protected static final int GRID_CNT = 2;

    /** */
    private static final int PERS_CNT = 600;

    /** */
    private static final int PURCHASE_CNT = 6_000;

    /** */
    private static final int COMPANY_CNT = 25;

    /** */
    private static final int PRODUCT_CNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        int i = 0;

        CacheConfiguration<?, ?>[] ccs = new CacheConfiguration[4];

        for (String name : F.asList("pe", "pu", "co", "pr")) {
            CacheConfiguration<?, ?> cc = defaultCacheConfiguration();

            cc.setName(name);
            cc.setCacheMode(PARTITIONED);
            cc.setBackups(2);
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setAtomicityMode(TRANSACTIONAL);
            cc.setRebalanceMode(SYNC);
            cc.setLongQueryWarningTimeout(15_000);
            cc.setAffinity(new RendezvousAffinityFunction(false, 60));

            switch (name) {
                case "pe":
                    cc.setIndexedTypes(
                            Integer.class, Person.class
                    );

                    break;

                case "pu":
                    cc.setIndexedTypes(
                            Integer.class, Purchase.class
                    );

                    break;

                case "co":
                    cc.setIndexedTypes(
                            Integer.class, Company.class
                    );

                    break;

                case "pr":
                    cc.setIndexedTypes(
                            Integer.class, Product.class
                    );

                    break;
            }

            ccs[i++] = cc;
        }

        c.setCacheConfiguration(ccs);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRID_CNT);

        fillCaches();
    }

    /**
     *
     */
    private void fillCaches() {
        IgniteCache<Integer, Company> co = grid(0).cache("co");

        for (int i = 0; i < COMPANY_CNT; i++)
            co.put(i, new Company(i));

        IgniteCache<Integer, Product> pr = grid(0).cache("pr");

        Random rnd = new GridRandom();

        for (int i = 0; i < PRODUCT_CNT; i++)
            pr.put(i, new Product(i, rnd.nextInt(COMPANY_CNT)));

        IgniteCache<Integer, Person> pe = grid(0).cache("pe");

        for (int i = 0; i < PERS_CNT; i++)
            pe.put(i, new Person(i));

        IgniteCache<Integer, Purchase> pu = grid(0).cache("pu");

        for (int i = 0; i < PURCHASE_CNT; i++) {
            int persId = rnd.nextInt(PERS_CNT);
            int prodId = rnd.nextInt(PRODUCT_CNT);

            pu.put(i, new Purchase(persId, prodId));
        }
    }

    /**
     *
     */
    protected static class Person implements Serializable {
        /** */
        @QuerySqlField(index = true)
        int id;

        /**
         * @param id ID.
         */
        Person(int id) {
            this.id = id;
        }
    }

    /**
     *
     */
    protected static class Purchase implements Serializable {
        /** */
        @QuerySqlField(index = true)
        int personId;

        /** */
        @QuerySqlField(index = true)
        int productId;

        /**
         * @param personId Person ID.
         * @param productId Product ID.
         */
        Purchase(int personId, int productId) {
            this.personId = personId;
            this.productId = productId;
        }
    }

    /**
     *
     */
    protected static class Company implements Serializable {
        /** */
        @QuerySqlField(index = true)
        int id;

        /**
         * @param id ID.
         */
        Company(int id) {
            this.id = id;
        }
    }

    /**
     *
     */
    protected static class Product implements Serializable {
        /** */
        @QuerySqlField(index = true)
        int id;

        /** */
        @QuerySqlField(index = true)
        int companyId;

        /**
         * @param id ID.
         * @param companyId Company ID.
         */
        Product(int id, int companyId) {
            this.id = id;
            this.companyId = companyId;
        }
    }

    /** */
    public static class Functions {
        /** */
        @QuerySqlFunction
        public static int sleep() {
            try {
                U.sleep(1_000);
            } catch (IgniteInterruptedCheckedException ignored) {
                // No-op.
            }

            return 0;
        }
    }
}
