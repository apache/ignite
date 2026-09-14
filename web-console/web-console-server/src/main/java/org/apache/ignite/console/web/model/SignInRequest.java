

package org.apache.ignite.console.web.model;

import java.util.UUID;
import jakarta.validation.constraints.NotNull;
import org.apache.ignite.internal.util.typedef.internal.S;
import jakarta.validation.constraints.NotEmpty;

/**
 * Web model of sign in request.
 */
public class SignInRequest {
    /** Email. */
    @NotNull
    @NotEmpty
    private String email;

    /** Password. */
    @NotNull
    @NotEmpty
    private String pwd;

    /** Activation token. */
    private UUID activationTok;

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
     * @return Activation token.
     */
    public UUID getActivationToken() {
        return activationTok;
    }

    /**
     * @param activationTok New activation token.
     */
    public void setActivationToken(UUID activationTok) {
        this.activationTok = activationTok;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SignInRequest.class, this);
    }
}
