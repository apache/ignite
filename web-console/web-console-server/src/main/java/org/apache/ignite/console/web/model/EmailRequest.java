

package org.apache.ignite.console.web.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import org.apache.ignite.internal.util.typedef.internal.S;
import jakarta.validation.constraints.NotEmpty;

/**
 * Web model of forgot password request.
 */
public class EmailRequest {
    /** Email. */
    @Schema(title = "User email.", required = true)
    @NotNull
    @NotEmpty
    private String email;

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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(EmailRequest.class, this);
    }
}
