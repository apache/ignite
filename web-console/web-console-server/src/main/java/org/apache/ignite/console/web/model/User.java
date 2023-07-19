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

package org.apache.ignite.console.web.model;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotNull;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Base class for user web models.
 */
public class User {
    /** Email. */
    @Schema(title = "User email", required = true)
    @NotNull
    @NotEmpty
    private String email;

    /** First name. */
    @Schema(title = "User first name", required = true)
    @NotNull
    @NotEmpty
    private String firstName;

    /** Last name. */
    @Schema(title = "User last name", required = true)
    @NotNull
    @NotEmpty
    private String lastName;

    /** Phone. */
    @Schema(title = "User phone")
    private String phone;

    /** Company. */
    @Schema(title = "User company", required = true)
    @NotNull
    @NotEmpty
    private String company;

    /** Country. */
    @Schema(title = "User country", required = true)
    @NotNull
    @NotEmpty
    private String country;

    /**
     * Default constructor for serialization.
     */
    public User() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param acc AccountDTO.
     */
    public User(Account acc) {
        email = acc.getEmail();
        firstName = acc.getFirstName();
        lastName = acc.getLastName();
        phone = acc.getPhone();
        company = acc.getCompany();
        country = acc.getCountry();
    }

    /**
     * @return Email.
     */
    public String getEmail() {
        return email;
    }

    /**
     * @param email New email.
     */
    public void setEmail(String email) {
        this.email = email;
    }

    /**
     * @return First name.
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @param firstName New first name.
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @return Last name.
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * @param lastName New last name.
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * @return Company.
     */
    public String getCompany() {
        return company;
    }

    /**
     * @param company New company.
     */
    public void setCompany(String company) {
        this.company = company;
    }

    /**
     * @return Country.
     */
    public String getCountry() {
        return country;
    }

    /**
     * @param country New country.
     */
    public void setCountry(String country) {
        this.country = country;
    }

    /**
     * @return Phone.
     */
    public String getPhone() {
        return phone;
    }

    /**
     * @param phone New phone.
     */
    public void setPhone(String phone) {
        this.phone = phone;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(User.class, this);
    }
}
