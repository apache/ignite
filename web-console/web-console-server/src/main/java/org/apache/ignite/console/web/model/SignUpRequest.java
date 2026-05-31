

package org.apache.ignite.console.web.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import org.apache.ignite.internal.util.typedef.internal.S;
import jakarta.validation.constraints.NotEmpty;

/**
 * Web model of sign up request.
 */
public class SignUpRequest extends User {
    /** Password. */
    @Schema(title = "User password.")
    @NotNull
    @NotEmpty
    private String pwd;

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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SignUpRequest.class, this);
    }
}
