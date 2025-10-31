package org.apache.ignite.tests.p2p.compute;

import java.io.Externalizable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteRunnable;

/** */
public class ExternalRunnableWithSerializableException implements IgniteRunnable {
    /** */
    private static final String MSG = "Message from Exception";

    /** */
    private static final int CODE = 127;

    /** */
    private static final String DETAILS = "Details from Exception";

    /** {@inheritDoc} */
    @Override public void run() {
        throw new SerializableException(MSG, CODE, DETAILS);
    }

    /** Custom {@link Externalizable} Exception */
    private static class SerializableException extends IgniteException {
        /** */
        protected int code;

        /** */
        protected String details;

        /** */
        public SerializableException() {
            // No-op.
        }

        /** */
        public SerializableException(String msg, int code, String details) {
            super(msg);

            this.code = code;
            this.details = details;
        }

        /** */
        public int getCode() {
            return code;
        }

        /** */
        public void setCode(int code) {
            this.code = code;
        }

        /** */
        public String getDetails() {
            return details;
        }

        /** */
        public void setDetails(String details) {
            this.details = details;
        }
    }
}
