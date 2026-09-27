

package org.apache.ignite.console.web.model;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.NotEmpty;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Web model of change user request.
 */
public class ChangeUserRequest extends User {
    /** Password. */
    @Schema(title = "User password.")
    private String pwd;

    /** Agent token. */
    @Schema(title = "Agent token.", required = true)
    @NotNull
    @NotEmpty
    private String tok;

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
}
