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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Web model of reset password request.
 */
public class ResetPasswordRequest {
    /** Email. */
    @Schema(title = "User email.", required = true)
    @NotNull
    @NotEmpty
    private String email;

    /** Password. */
    @Schema(title = "User password.", required = true)
    @NotNull
    @NotEmpty
    private String pwd;

    /** Reset password token. */
    @Schema(title = "Reset password token.", required = true)
    @NotNull
    @NotEmpty
    private String tok;

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
     * @return Password.
     */
    public String getPassword() {
        return pwd;
    }

    /**
     * @param pwd New password.
     */
    public void setPassword(String pwd) {
        this.pwd = pwd;
    }

    /**
     * @return Reset password token.
     */
    public String getToken() {
        return tok;
    }

    /**
     * @param tok Reset password token.
     */
    public void setToken(String tok) {
        this.tok = tok;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ResetPasswordRequest.class, this);
    }
}
