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
package org.apache.ignite.springdata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.springdata.compoundkey.City;
import org.apache.ignite.springdata.compoundkey.CityKey;
import org.apache.ignite.springdata.compoundkey.CityRepository;
import org.apache.ignite.springdata.compoundkey.CompoundKeyApplicationConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.apache.ignite.springdata.compoundkey.CompoundKeyApplicationConfiguration.CLI_CONN_PORT;

/**
 * Test with using conpoud key in spring-data
 * */
public class IgniteSpringDataCompoundKeyTest extends GridCommonAbstractTest {
    /** Application context */
    protected static AnnotationConfigApplicationContext ctx;

    /** City repository */
    protected static CityRepository repo;

    /** Cache name */
    private static final String CACHE_NAME = "City";

    /** Cities count */
    private static final int TOTAL_COUNT = 5;

    /** Count Afganistan cities */
    private static final int AFG_COUNT = 4;

    /** Kabul identifier */
    private static final int KABUL_ID = 1;

    /** Quandahar identifier */
    private static final int QUANDAHAR_ID = 2;

    /** Afganistan county code */
    private static final String AFG = "AFG";

    /** test city Kabul */
    protected static final City KABUL = new City("Kabul", "Kabol", 1780000);

    /** test city Quandahar */
    private static final City QUANDAHAR = new City("Qandahar", "Qandahar", 237500);

    /**
     * Performs context initialization before tests.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ctx = new AnnotationConfigApplicationContext();
        ctx.register(CompoundKeyApplicationConfiguration.class);
        ctx.refresh();

        repo = ctx.getBean(CityRepository.class);
    }

    /**
     * Load data
     * */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        loadData();

        assertEquals(getTotalCount(), repo.count());
    }

    /**
     * Clear data
     * */
    @Override protected void afterTest() throws Exception {
        repo.deleteAll();

        assertEquals(0, repo.count());

        super.afterTest();
    }

    /**
     * Performs context destroy after tests.
     */
    @Override protected void afterTestsStopped() {
        ctx.close();
    }

    /** Load data. */
    public void loadData() throws Exception {
        Ignite ignite = ignite();

        if (ignite.cacheNames().contains(CACHE_NAME))
            ignite.destroyCache(CACHE_NAME);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:" + CLI_CONN_PORT + '/')) {
            Statement st = conn.createStatement();

            st.execute("DROP TABLE IF EXISTS City");
            st.execute("CREATE TABLE City (ID INT, Name VARCHAR, CountryCode CHAR(3), District VARCHAR, Population INT, PRIMARY KEY " +
                "(ID, CountryCode)) WITH \"template=partitioned, backups=1, affinityKey=CountryCode, CACHE_NAME=City, " +
                "KEY_TYPE=org.apache.ignite.springdata.compoundkey.CityKey, VALUE_TYPE=org.apache.ignite.springdata.compoundkey.City\"");
            st.execute("SET STREAMING ON;");
            st.execute("INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (1,'Kabul','AFG','Kabol',1780000)");
            st.execute("INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (2,'Qandahar','AFG','Qandahar',237500)");
            st.execute("INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (3,'Herat','AFG','Herat',186800)");
            st.execute("INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (4,'Mazar-e-Sharif','AFG','Balkh',127800)");
            st.execute("INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (5,'Amsterdam','NLD','Noord-Holland',731200)");
        }
    }

    /** Test */
    @Test
    public void test() {
        assertEquals(Optional.of(KABUL), repo.findById(new CityKey(KABUL_ID, AFG)));
        assertEquals(AFG_COUNT, repo.findByCountryCode(AFG).size());
        assertEquals(QUANDAHAR, repo.findById(QUANDAHAR_ID));
    }

    /** Test. */
    @Test
    public void deleteAllById() {
        Set<CityKey> keys = new HashSet<>();
        keys.add(new CityKey(1, "AFG"));
        keys.add(new CityKey(2, "AFG"));
        keys.add(new CityKey(3, "AFG"));
        keys.add(new CityKey(4, "AFG"));
        keys.add(new CityKey(5, "NLD"));

        repo.deleteAllById(keys);
        assertEquals(0, repo.count());
    }

    /** */
    protected Ignite ignite() {
        return ctx.getBean(Ignite.class);
    }

    /** Total count of entries after load data. */
    protected int getTotalCount() {
        return TOTAL_COUNT;
    }
}
