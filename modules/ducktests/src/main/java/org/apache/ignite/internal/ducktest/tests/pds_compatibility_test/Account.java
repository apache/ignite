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

package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;

import java.io.Serializable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Account implements Serializable {

    @QuerySqlField(index = true, inlineSize = 48)
    private final String firstName;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String lastName;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String catName;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String dogName;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String city;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String country;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String eMail;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String phoneNumber;

    @QuerySqlField(index = true, inlineSize = 48)
    private final String socialNumber;

    @QuerySqlField(index = true, inlineSize = 48)
    private final Long postIndex;

    public final long balance;

    public Account(String firstName, String lastName, String catName, String dogName, String city, String country, String eMail, String phoneNumber, String socialNumber, Long postIndex, long balance) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.catName = catName;
        this.dogName = dogName;
        this.city = city;
        this.country = country;
        this.eMail = eMail;
        this.phoneNumber = phoneNumber;
        this.socialNumber = socialNumber;
        this.postIndex = postIndex;
        this.balance = balance;
    }

    @Override public String toString() {
        return "Account{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", catName='" + catName + '\'' +
                ", dogName='" + dogName + '\'' +
                ", city='" + city + '\'' +
                ", country='" + country + '\'' +
                ", eMail='" + eMail + '\'' +
                ", phoneNumber='" + phoneNumber + '\'' +
                ", socialNumber='" + socialNumber + '\'' +
                ", postIndex='" + postIndex + '\'' +
                ", balance=" + balance +
                '}';
    }
}
