/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql.model;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import static org.apache.ignite.compatibility.sql.model.City.Factory.CITY_CNT;
import static org.apache.ignite.compatibility.sql.model.Company.Factory.COMPANY_CNT;
import static org.apache.ignite.compatibility.sql.model.ModelUtil.randomString;

/**
 * Department model.
 */
public class Department {
    /** Name. */
    @QuerySqlField
    private final String name;

    /** Head count. */
    @QuerySqlField
    private final int headCnt;

    /** City id. */
    @QuerySqlField
    private final int cityId;

    /** Company id. */
    @QuerySqlField
    private final int companyId;

    /** */
    public Department(String name, int headCnt, int cityId, int companyId) {
        this.name = name;
        this.headCnt = headCnt;
        this.cityId = cityId;
        this.companyId = companyId;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public int headCount() {
        return headCnt;
    }

    /** */
    public int cityId() {
        return cityId;
    }

    /** */
    public int companyId() {
        return companyId;
    }

    /** */
    public static class Factory implements ModelFactory {
        /** Table name. */
        private static final String TABLE_NAME = "department";

        /** */
        public static final int DEPS_CNT = 1000;

        /** */
        private final Random rnd;

        /** */
        private final QueryEntity qryEntity;

        /**
         * @param seed Seed.
         */
        public Factory(int seed) {
            this.rnd = new Random(seed);
            QueryEntity entity = new QueryEntity(Long.class, Department.class);
            entity.setKeyFieldName("id");
            entity.addQueryField("id", Long.class.getName(), null);
            List<QueryIndex> indices = Arrays.asList(
                new QueryIndex("companyId", QueryIndexType.SORTED),
                new QueryIndex(Arrays.asList("cityId", "headCnt"), QueryIndexType.SORTED)
            );
            entity.setIndexes(indices);
            entity.setTableName(TABLE_NAME);
            this.qryEntity = entity;
        }

        /** {@inheritDoc} */
        @Override public Department createRandom() {
            return new Department(
                randomString(rnd, 5, 10), // name
                rnd.nextInt(20), // head count
                rnd.nextInt(CITY_CNT), // city id
                rnd.nextInt(COMPANY_CNT) // company id
            );
        }

        /** {@inheritDoc} */
        @Override public QueryEntity queryEntity() {
            return qryEntity;
        }

        /** {@inheritDoc} */
        @Override public String tableName() {
            return TABLE_NAME;
        }

        /** {@inheritDoc} */
        @Override public int count() {
            return DEPS_CNT;
        }
    }
}
