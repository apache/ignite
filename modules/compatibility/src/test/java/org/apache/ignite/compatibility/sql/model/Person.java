/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql.model;

import java.util.Collections;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * TODO: Add class description.
 */
public class Person {
    @QuerySqlField
    private final String name;

    @QuerySqlField
    private final int depId;

    public Person(String name, int depId) {
        this.name = name;
        this.depId = depId;
    }

    public static class PersonFactory implements ModelFactory {
        private final String TABLE_NAME = "person";
        private static final String[] NAMES = new String[] {"Ivan", "Semyon", "Valera"};
        private final Random random;
        private final QueryEntity queryEntity;

        public PersonFactory(int seed) {
            this.random = new Random(seed);
            QueryEntity entity = new QueryEntity(Long.class, Person.class);
            Set<QueryIndex> indices = Collections.singleton(new QueryIndex("depId", QueryIndexType.SORTED));
            entity.setIndexes(indices);
            entity.setTableName(TABLE_NAME);
            this.queryEntity = entity;
        }

        /** {@inheritDoc} */
        @Override public Person createRandom() {
            return new Person(
                NAMES[random.nextInt(NAMES.length)],
                random.nextInt(10)
            );
        }

        /** {@inheritDoc} */
        @Override public QueryEntity queryEntity() {
            return queryEntity;
        }

        /** {@inheritDoc} */
        @Override public String tableName() {
            return TABLE_NAME;
        }
    }
}
