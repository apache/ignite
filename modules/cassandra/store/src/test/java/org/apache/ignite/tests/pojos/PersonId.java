/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.tests.pojos;

import java.io.Serializable;

/**
 * Simple POJO which could be stored as a key in Ignite cache
 */
public class PersonId implements Serializable {
    /** */
    private String companyCode;

    /** */
    private String departmentCode;

    /** */
    private long personNum;

    /** */
    @SuppressWarnings("UnusedDeclaration")
    public PersonId() {
    }

    /** */
    public PersonId(String companyCode, String departmentCode, long personNum) {
        this.companyCode = companyCode;
        this.departmentCode = departmentCode;
        this.personNum = personNum;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof PersonId))
            return false;

        PersonId id = (PersonId)obj;

        if ((companyCode != null && !companyCode.equals(id.companyCode)) ||
            (id.companyCode != null && !id.companyCode.equals(companyCode)))
            return false;

        if ((companyCode != null && !companyCode.equals(id.companyCode)) ||
            (id.companyCode != null && !id.companyCode.equals(companyCode)))
            return false;

        return personNum == id.personNum;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        String code = (companyCode == null ? "" : companyCode) +
            (departmentCode == null ? "" : departmentCode) +
                personNum;

        return code.hashCode();
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    public void setCompanyCode(String code) {
        companyCode = code;
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    public String getCompanyCode() {
        return companyCode;
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    public void setDepartmentCode(String code) {
        departmentCode = code;
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    public String getDepartmentCode() {
        return departmentCode;
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    public void setPersonNumber(long personNum) {
        this.personNum = personNum;
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    public long getPersonNumber() {
        return personNum;
    }
}
