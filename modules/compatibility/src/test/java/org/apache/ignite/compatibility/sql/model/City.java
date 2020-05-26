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

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * TODO: Add class description.
 */
public class City {
    @QuerySqlField
    private final String name;

    @QuerySqlField
    private final int zipCode;

    @QuerySqlField
    private final int countryId;

    public City(String name, int zipCode, int countryId) {
        this.name = name;
        this.zipCode = zipCode;
        this.countryId = countryId;
    }
}
