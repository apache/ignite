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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;

/**
 * Tests for local query execution in lazy mode.
 */
public class LocalQueryLazyTest2 extends AbstractIndexingCommonTest {
    private static final String QUERY =
        "SELECT DISTINCT ID, DATEOFBIRTH, FIRSTNAMEUPPER, LASTNAMEUPPER " +
            "FROM CONTACT USE INDEX (CONTACT_LASTNAMEUPPER_FIRSTNAMEUPPER_DATEOFBIRTH_ASC_IDX) " +
            "WHERE (FIRSTNAMEUPPER LIKE ?) AND (LASTNAMEUPPER LIKE ?);";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setLongQueryWarningTimeout(0)
            .setClientMode(igniteInstanceName.startsWith("cli"));
    }

    /**
     * Test local query execution.
     */
    public void test() throws Exception {

        startGrid("srv");
        startGrid("cli");

        IgniteCache def = grid("cli").createCache("default");

        def.query(new SqlFieldsQuery(
            "CREATE TABLE CONTACT (ID INT PRIMARY KEY, DATEOFBIRTH INT, FIRSTNAMEUPPER VARCHAR, LASTNAMEUPPER VARCHAR);")
            .setSchema("PUBLIC"));

        for (int i = 0; i < 10_000; i++)
            def.query(new SqlFieldsQuery("INSERT INTO CONTACT (ID, DATEOFBIRTH, FIRSTNAMEUPPER, LASTNAMEUPPER) VALUES (?, ?, ?, ?);")
                .setArgs(
                    i,
                    ThreadLocalRandom.current().nextInt(1000),
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString())
                .setSchema("PUBLIC"));

        def.query(new SqlFieldsQuery(
            "CREATE INDEX CONTACT_LASTNAMEUPPER_FIRSTNAMEUPPER_DATEOFBIRTH_ASC_IDX ON CONTACT (LASTNAMEUPPER, FIRSTNAMEUPPER, DATEOFBIRTH)")
            .setSchema("PUBLIC"));

        SqlFieldsQuery qry = new SqlFieldsQuery(QUERY).setSchema("PUBLIC").setArgs("JO%", "SM%");

        List<List<?>> result = def.query(qry).getAll();

        System.out.println(result.size());
    }
}
