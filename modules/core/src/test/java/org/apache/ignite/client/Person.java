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

package org.apache.ignite.client;

import java.util.Objects;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * A person entity used for the tests.
 */
public class Person {
    /** Id. */
    @QuerySqlField(index = true)
    private final Integer id;

    /** Name. */
    @QuerySqlField
    private final String name;

    /** Constructor. */
    public Person(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    /** @return id. */
    public Integer getId() {
        return id;
    }

    /** @return name. */
    public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(id, name);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof Person))
            return false;

        Person other = (Person)obj;

        return other.id.equals(id) && other.name.equals(name);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Person.class, this);
    }
}
