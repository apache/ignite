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
public class PersonEx extends Person {
    /**
     * Additional information.
     */
    @QuerySqlField
    private final String info;

    /**
     * Description - not declared as SQL field.
     */
    private final String description;

    /**
     * Constructor.
     */
    public PersonEx(Integer id, String name, String info, String description) {
        super(id, name);
        this.info = info;
        this.description = description;
    }

    /**
     * @return Additional information.
     */
    public String getInfo() {
        return info;
    }

    /**
     * @return Description.
     */
    public String getDescription() {
        return description;
    }

    /**
     * {@inheritDoc}
     */
    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), info, description);
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof PersonEx))
            return false;

        PersonEx other = (PersonEx)obj;

        return super.equals(other) && Objects.equals(info, other.info) && Objects.equals(description, other.description);
    }
}
