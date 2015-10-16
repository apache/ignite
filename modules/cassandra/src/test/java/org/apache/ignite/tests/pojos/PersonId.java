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

import java.io.Serializable;

/**
 * Simple POJO which could be stored as a key in Ignite cache
 */
public class PersonId implements Serializable {
    private String companyCode;
    private String departmentCode;
    private int personNumber;

    @SuppressWarnings("UnusedDeclaration")
    public PersonId() {
    }

    public PersonId(String companyCode, String departmentCode, int personNumber) {
        this.companyCode = companyCode;
        this.departmentCode = departmentCode;
        this.personNumber = personNumber;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof PersonId))
            return false;

        PersonId id = (PersonId)obj;

        if ((companyCode != null && !companyCode.equals(id.companyCode)) ||
            (id.companyCode != null && !id.companyCode.equals(companyCode))) {
            return false;
        }

        if ((companyCode != null && !companyCode.equals(id.companyCode)) ||
            (id.companyCode != null && !id.companyCode.equals(companyCode))) {
            return false;
        }

        return personNumber == id.personNumber;
    }

    @Override public int hashCode() {
        String code = (companyCode == null ? "" : companyCode) +
            (departmentCode == null ? "" : departmentCode) +
            personNumber;

        return code.hashCode();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setCompanyCode(String code) {
        companyCode = code;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getCompanyCode() {
        return companyCode;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setDepartmentCode(String code) {
        departmentCode = code;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getDepartmentCode() {
        return departmentCode;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setPersonNumber(int number) {
        personNumber = number;
    }

    @SuppressWarnings("UnusedDeclaration")
    public int getPersonNumber() {
        return personNumber;
    }
}
