

package org.apache.ignite.console.web.model;

/**
 * Error with email response.
 */
public class ErrorWithEmailResponse extends ErrorResponse {
    /** */
    private String email;

    /**
     * Default constructor for serialization.
     */
    public ErrorWithEmailResponse() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param code Error code.
     * @param msg Error message.
     * @param username User name.
     */
    public ErrorWithEmailResponse(int code, String msg, String username) {
        super(code, msg);

        email = username;
    }

    /**
     * @return Email.
     */
    public String getEmail() {
        return email;
    }

    /**
     * @param email Email.
     */
    public void setEmail(String email) {
        this.email = email;
    }
}
