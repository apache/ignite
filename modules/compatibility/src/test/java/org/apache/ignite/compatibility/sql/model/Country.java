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

import static org.apache.ignite.compatibility.sql.model.ModelUtil.randomString;

/**
 * Country model.
 */
public class Country {
    /** */
    @QuerySqlField
    private final String name;

    /** */
    @QuerySqlField
    private final String phoneCode;

    /** */
    @QuerySqlField
    private final int population;

    /** */
    public Country(String name, String phoneCode, int population) {
        this.name = name;
        this.phoneCode = phoneCode;
        this.population = population;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public String phoneCode() {
        return phoneCode;
    }

    /** */
    public int population() {
        return population;
    }

    /** */
    public static class Factory implements ModelFactory {
        /** Table name. */
        private static final String TABLE_NAME = "country";

        /** */
        public static final int COUNTRY_CNT = 50;

        /** */
        private Random rnd;

        /** */
        private final QueryEntity qryEntity;

        /** */
        public Factory() {
            QueryEntity entity = new QueryEntity(Long.class, Country.class);
            entity.setKeyFieldName("id");
            entity.addQueryField("id", Long.class.getName(), null);
            List<QueryIndex> indices = Arrays.asList(
                new QueryIndex(Arrays.asList("name"), QueryIndexType.SORTED)
            );
            entity.setIndexes(indices);
            entity.setTableName(TABLE_NAME);
            this.qryEntity = entity;
        }

        @Override public void init(int seed) {
            this.rnd = new Random(seed);
        }

        /** {@inheritDoc} */
        @Override public Country createRandom() {
            if (rnd == null) {
                throw new IllegalStateException("Factory is not initialized with a random seed. " +
                    "Call Factory.iniFactory(int seed) before using this method.");
            }
            return new Country(
                randomString(rnd, 5, 10), // name
                randomString(rnd, 3, 3), // phone code
                rnd.nextInt(1_000) // population
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
            return COUNTRY_CNT;
        }
    }
}
