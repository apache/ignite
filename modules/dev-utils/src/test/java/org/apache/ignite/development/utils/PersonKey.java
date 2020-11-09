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

package org.apache.ignite.development.utils;

import java.util.Objects;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * A person entity used for the tests.
 */
public class PersonKey {
    /**
     * Id.
     */
    @QuerySqlField(index = true)
    private final Integer id;

    /**
     * Constructor.
     */
    public PersonKey(Integer id) {
        this.id = id;
    }

    /**
     * @return id.
     */
    public Integer getId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof PersonKey))
            return false;

        PersonKey other = (PersonKey)obj;

        return Objects.equals(other.id, id);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(id);
    }
}
