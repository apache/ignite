

package org.apache.ignite.console.web.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import org.apache.ignite.internal.util.typedef.internal.S;
import jakarta.validation.constraints.NotEmpty;

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
