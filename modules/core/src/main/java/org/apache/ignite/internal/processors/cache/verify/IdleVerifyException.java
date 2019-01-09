package org.apache.ignite.internal.processors.cache.verify;

import java.util.Collection;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;

/**
 * This exception is used to collect exceptions occured in {@link VerifyBackupPartitionsTaskV2} execution.
 */
public class IdleVerifyException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Occured exceptions. */
    private final Collection<IgniteException> exceptions;

    /** */
    public IdleVerifyException(Collection<IgniteException> exceptions) {
        if(F.isEmpty(exceptions))
            throw new IllegalArgumentException("Exceptions can't be empty!");

        this.exceptions = exceptions;
    }

    /** */
    public IdleVerifyException(String msg, Collection<IgniteException> exceptions) {
        super(msg);

        if(F.isEmpty(exceptions))
            throw new IllegalArgumentException("Exceptions can't be empty!");

        this.exceptions = exceptions;
    }

    /**
     * @return Exceptions.
     */
    public Collection<IgniteException> exceptions() {
        return exceptions;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() +": " + getMessage() + " exceptions: " + exceptions;
    }
}
