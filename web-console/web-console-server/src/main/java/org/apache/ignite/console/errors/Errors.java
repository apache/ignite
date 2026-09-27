

package org.apache.ignite.console.errors;

import javax.cache.CacheException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;

/**
 * Class with error codes and messages.
 */
public class Errors {
    /** */
    public static final int ERR_EMAIL_NOT_CONFIRMED = 10104;

    /**
     * @param e Exception to check.
     * @return {@code true} if database not available.
     */
    public static boolean checkDatabaseNotAvailable(Throwable e) {
        if (e instanceof IgniteClientDisconnectedException)
            return true;

        if (e instanceof CacheException && e.getCause() instanceof IgniteClientDisconnectedException)
            return true;

        // TODO GG-19681: In future versions specific exception will be added.
        String msg = e.getMessage();

        return e instanceof IgniteException &&
            msg != null &&
            msg.startsWith("Cannot start/stop cache within lock or transaction");
    }


    /**
     * @param e Exception.
     * @param msg Message.
     * @return Exception to throw.
     */
    public static RuntimeException convertToDatabaseNotAvailableException(RuntimeException e, String msg) {
        return checkDatabaseNotAvailable(e) ? new DatabaseNotAvailableException(msg) : e;
    }
}
