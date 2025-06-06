

package org.apache.ignite.console.web.model;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.springframework.http.HttpStatus;

/**
 * Error response JSON bean with the error code and technical withError message.
 */
public class ErrorResponse {
    /** */
    private int code;

    /** */
    private String msg;

    /**
     * Default constructor for serialization.
     */
    public ErrorResponse() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param code Error code.
     * @param msg Error message.
     */
    public ErrorResponse(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    /**
     * Full constructor.
     *
     * @param status HTTP status.
     * @param msg Error message.
     */
    public ErrorResponse(HttpStatus status, String msg) {
        this(status.value(), msg);
    }

    /**
     * @return Error code.
     */
    public int getCode() {
        return code;
    }

    /**
     * @param code Error code.
     */
    public void setCode(int code) {
        this.code = code;
    }

    /**
     * @return Error message.
     */
    public String getMessage() {
        return msg;
    }

    /**
     * @param msg Error message.
     */
    public void setMessage(String msg) {
        this.msg = msg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ErrorResponse.class, this);
    }
}
