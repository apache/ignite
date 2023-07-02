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

import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.NotNull;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Web model of the user.
 */
public class UserResponse extends User {
    /** Agent token. */
    @ApiModelProperty(value = "Agent token.")
    @NotNull
    @NotEmpty
    private String tok;

    /** Admin flag. */
    @ApiModelProperty(value = "Admin flag.")
    private boolean admin;

    /** Switch user used flag. */
    @ApiModelProperty(value = "Switch user used flag.")
    private boolean becomeUsed;

    /**
     * Default constructor for serialization.
     */
    public UserResponse() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param acc Account DTO.
     * @param becomeUsed Switch user used flag.
     */
    public UserResponse(Account acc, boolean becomeUsed) {
        super(acc);

        this.tok = acc.getToken();
        this.admin = acc.isAdmin();
        this.becomeUsed = becomeUsed;
    }

    /**
     * @return Agent token.
     */
    public String getToken() {
        return tok;
    }

    /**
     * @param tok New agent token.
     */
    public void setToken(String tok) {
        this.tok = tok;
    }

    /**
     * @return Admin flag.
     */
    public boolean isAdmin() {
        return admin;
    }

    /**
     * @param admin Admin flag.
     */
    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    /**
     * @return Switch user used flag
     */
    public boolean isBecomeUsed() {
        return becomeUsed;
    }

    /**
     * @param becomeUsed Switch user used flag.
     */
    public void setBecomeUsed(boolean becomeUsed) {
        this.becomeUsed = becomeUsed;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserResponse.class, this);
    }
}
