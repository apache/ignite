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

/**
 * Simple POJO which could be stored as a value in Ignite cache
 */
public class Person implements Externalizable {
    /** */
    private long personNum;

    /** */
    private String firstName;

    /** */
    private String lastName;

    /** */
    private String fullName;

    /** */
    private short age;

    /** */
    private boolean married;

    /** */
    private long height;

    /** */
    private float weight;

    /** */
    private Date birthDate;

    /** */
    private List<String> phones;

    /** */
    public Person() {
    }

    /** */
    public Person(long personNum, String firstName, String lastName, short age, boolean married,
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
        if (obj == null || !(obj instanceof Person))
            return false;

        Person person = (Person)obj;

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
        if (obj == null || !(obj instanceof Person))
            return false;

        Person person = (Person)obj;

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

    /** */
    public void setPersonNumber(long personNum) {
        this.personNum = personNum;
    }

    /** */
    public long getPersonNumber() {
        return personNum;
    }

    /** */
    public void setFirstName(String name) {
        firstName = name;
        fullName = firstName + " " + lastName;
    }

    /** */
    public String getFirstName() {
        return firstName;
    }

    /** */
    public void setLastName(String name) {
        lastName = name;
        fullName = firstName + " " + lastName;
    }

    /** */
    public String getLastName() {
        return lastName;
    }

    /** */
    public String getFullName() {
        return fullName;
    }

    /** */
    public void setAge(short age) {
        this.age = age;
    }

    /** */
    public short getAge() {
        return age;
    }

    /** */
    public void setMarried(boolean married) {
        this.married = married;
    }

    /** */
    public boolean getMarried() {
        return married;
    }

    /** */
    public void setHeight(long height) {
        this.height = height;
    }

    /** */
    public long getHeight() {
        return height;
    }

    /** */
    public void setWeight(float weight) {
        this.weight = weight;
    }

    /** */
    public float getWeight() {
        return weight;
    }

    /** */
    public void setBirthDate(Date date) {
        birthDate = date;
    }

    /** */
    public Date getBirthDate() {
        return birthDate;
    }

    /** */
    public void setPhones(List<String> phones) {
        this.phones = phones;
    }

    /** */
    public List<String> getPhones() {
        return phones;
    }
}
