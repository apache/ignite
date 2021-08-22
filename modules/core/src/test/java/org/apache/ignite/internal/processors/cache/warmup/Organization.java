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
 * Organization.
 */
class Organization {
    /** Id. */
    final long id;

    /** Name. */
    final String name;

    /**
     * Constructor.
     *
     * @param id Id.
     * @param name Name.
     */
    Organization(long id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * Create query entity.
     *
     * @return New query entity.
     */
    static QueryEntity queryEntity() {
        return new QueryEntity(String.class, Organization.class)
            .addQueryField("id", Long.class.getName(), null)
            .addQueryField("name", String.class.getName(), null)
            .setIndexes(Arrays.asList(
                new QueryIndex("id"),
                new QueryIndex("name")
            ));
    }
}
