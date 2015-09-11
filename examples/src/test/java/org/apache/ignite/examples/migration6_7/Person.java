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

package org.apache.ignite.examples.migration6_7;

import java.io.Serializable;
import java.util.UUID;

/**
 */
public class Person implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    PersonId id = new PersonId();
    String name = UUID.randomUUID().toString();
    String name2 = UUID.randomUUID().toString();
    Organization org;

    public Person(Organization org) {
        this.org = org;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Person))
            return false;

        Person person = (Person)o;

        if (id != null ? !id.equals(person.id) : person.id != null)
            return false;
        if (name != null ? !name.equals(person.name) : person.name != null)
            return false;
        if (name2 != null ? !name2.equals(person.name2) : person.name2 != null)
            return false;
        if (org != null ? !org.equals(person.org) : person.org != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (name2 != null ? name2.hashCode() : 0);
        result = 31 * result + (org != null ? org.hashCode() : 0);
        return result;
    }
}
