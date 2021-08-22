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

package org.apache.ignite.internal.processors.cache.warmup;

import java.util.Arrays;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;

/**
 * Person.
 */
class Person {
    /** Id. */
    final long id;

    /** Name. */
    final String name;

    /** Organization id. */
    final long orgId;

    /**
     * Constructor.
     *
     * @param id Id.
     * @param name Name.
     * @param orgId Organization id.
     */
    Person(long id, String name, long orgId) {
        this.id = id;
        this.name = name;
        this.orgId = orgId;
    }

    /**
     * Create query entity.
     *
     * @return New query entity.
     */
    static QueryEntity queryEntity() {
        return new QueryEntity(String.class, Person.class)
            .addQueryField("id", Long.class.getName(), null)
            .addQueryField("name", String.class.getName(), null)
            .addQueryField("orgId", Long.class.getName(), null)
            .setIndexes(Arrays.asList(
                new QueryIndex("id"),
                new QueryIndex("name"),
                new QueryIndex("orgId")
            ));
    }
}
