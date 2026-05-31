

package org.apache.ignite.console.errors;

import org.eclipse.jetty.http.BadMessageException;

import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;

/**
 * Special exception to handle database errors.
 */
public class DatabaseNotAvailableException extends BadMessageException {
    /**
     * Default constructor.
     */
    public DatabaseNotAvailableException(String message) {
        super(SERVICE_UNAVAILABLE.value(), message);
    }
}
