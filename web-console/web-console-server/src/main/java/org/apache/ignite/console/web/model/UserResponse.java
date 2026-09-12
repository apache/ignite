

package org.apache.ignite.console.web.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.internal.util.typedef.internal.S;
import jakarta.validation.constraints.NotEmpty;

/**
 * Web model of the user.
 */
public class UserResponse extends User {
    /** Agent token. */
    @Schema(title = "Agent token.")
    @NotNull
    @NotEmpty
    private String tok;

    /** Admin flag. */
    @Schema(title = "Admin flag.")
    private boolean admin;

    /** Switch user used flag. */
    @Schema(title = "Switch user used flag.")
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
