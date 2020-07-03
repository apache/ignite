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

package org.apache.ignite.tests.pojos;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;
import java.util.List;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Simple POJO without getters/setters which could be stored as a value in Ignite cache
 */
public class SimplePerson implements Externalizable {
    /** */
    @QuerySqlField(name = "person_num")
    private long personNum;

    /** */
    @QuerySqlField(name = "first_name")
    private String firstName;

    /** */
    @QuerySqlField(name = "last_name")
    private String lastName;

    /** */
    @QuerySqlField(name = "age")
    private short age;

    /** */
    @QuerySqlField(name = "married", index = true)
    private boolean married;

    /** */
    @QuerySqlField(name = "height")
    private long height;

    /** */
    @QuerySqlField(name = "weight")
    private float weight;

    /** */
    @QuerySqlField(name = "birth_date")
    private Date birthDate;

    /** */
    @QuerySqlField(name = "phones")
    private List<String> phones;

    /** */
    public SimplePerson() {
    }

    /** */
    public SimplePerson(Person person) {
        this.personNum = person.getPersonNumber();
        this.firstName = person.getFirstName();
        this.lastName = person.getLastName();
        this.age = person.getAge();
        this.married = person.getMarried();
        this.height = person.getHeight();
        this.weight = person.getWeight();
        this.birthDate = person.getBirthDate();
        this.phones = person.getPhones();
    }

    /** */
    public SimplePerson(long personNum, String firstName, String lastName, short age, boolean married,
                        long height, float weight, Date birthDate, List<String> phones) {
        this.personNum = personNum;
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.married = married;
        this.height = height;
        this.weight = weight;
        this.birthDate = birthDate;
        this.phones = phones;
    }


    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(personNum);
        out.writeObject(firstName);
        out.writeObject(lastName);
        out.writeShort(age);
        out.writeBoolean(married);
        out.writeLong(height);
        out.writeFloat(weight);
        out.writeObject(birthDate);
        out.writeObject(phones);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        personNum = in.readLong();
        firstName = (String)in.readObject();
        lastName = (String)in.readObject();
        age = in.readShort();
        married = in.readBoolean();
        height = in.readLong();
        weight = in.readFloat();
        birthDate = (Date)in.readObject();
        phones = (List<String>)in.readObject();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SimplePerson))
            return false;

        SimplePerson person = (SimplePerson)obj;

        if (personNum != person.personNum)
            return false;

        if ((firstName != null && !firstName.equals(person.firstName)) ||
            (person.firstName != null && !person.firstName.equals(firstName)))
            return false;

        if ((lastName != null && !lastName.equals(person.lastName)) ||
            (person.lastName != null && !person.lastName.equals(lastName)))
            return false;

        if ((birthDate != null && !birthDate.equals(person.birthDate)) ||
            (person.birthDate != null && !person.birthDate.equals(birthDate)))
            return false;

        if ((phones != null && !phones.equals(person.phones)) ||
            (person.phones != null && !person.phones.equals(phones)))
            return false;

        return age == person.age && married == person.married &&
            height == person.height && weight == person.weight;
    }

    /** */
    @SuppressWarnings("SimplifiableIfStatement")
    public boolean equalsPrimitiveFields(Object obj) {
        if (obj == null || !(obj instanceof SimplePerson))
            return false;

        SimplePerson person = (SimplePerson)obj;

        if (personNum != person.personNum)
            return false;

        if ((firstName != null && !firstName.equals(person.firstName)) ||
            (person.firstName != null && !person.firstName.equals(firstName)))
            return false;

        if ((lastName != null && !lastName.equals(person.lastName)) ||
            (person.lastName != null && !person.lastName.equals(lastName)))
            return false;

        if ((birthDate != null && !birthDate.equals(person.birthDate)) ||
            (person.birthDate != null && !person.birthDate.equals(birthDate)))
            return false;

        return age == person.age && married == person.married &&
            height == person.height && weight == person.weight;
    }
}
