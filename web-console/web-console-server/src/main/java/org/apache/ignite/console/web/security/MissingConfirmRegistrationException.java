

package org.apache.ignite.console.web.security;

import org.springframework.security.authentication.DisabledException;

/**
 * Thrown if an authentication request is rejected because the account is disabled.
 */
public class MissingConfirmRegistrationException extends DisabledException {
    /** User name. */
    private String username;

    /**
     * @param msg Message.
     * @param username User name.
     */
    public MissingConfirmRegistrationException(String msg, String username) {
        super(msg);

        this.username = username;
    }

    /**
     * @return User name of disabled account.
     */
    public String getUsername() {
        return username;
    }
}
