package org.apache.ignite.tests.p2p.compute;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteRunnable;

/** */
public class ExternalRunnableWithExternalizableException implements IgniteRunnable {
    /** */
    private static final String MSG = "Message from Exception";

    /** */
    private static final int CODE = 127;

    /** */
    private static final String DETAILS = "Details from Exception";

    /** {@inheritDoc} */
    @Override public void run() {
        throw new ExternalizableException(MSG, CODE, DETAILS);
    }

    /** Custom {@link Externalizable} Exception */
    private static class ExternalizableException extends IgniteException implements Externalizable {
        /** */
        protected int code;

        /** */
        protected String details;

        /** */
        public ExternalizableException() {
            // No-op.
        }

        /** */
        public ExternalizableException(String msg, int code, String details) {
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

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(getMessage());
            out.writeInt(code);
            out.writeObject(details);
            out.writeObject(getStackTrace());
            out.writeObject(getCause());

            Throwable[] suppressed = getSuppressed();

            out.writeInt(suppressed.length);

            for (Throwable t : suppressed)
                out.writeObject(t);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            String msg = (String)in.readObject();

            try {
                Field detailMsg = Throwable.class.getDeclaredField("detailMessage");

                detailMsg.setAccessible(true);
                detailMsg.set(this, msg);
            }
            catch (Exception ignored) {
                // No-op.
            }

            code = in.readInt();
            details = (String)in.readObject();

            setStackTrace((StackTraceElement[])in.readObject());

            Throwable cause = (Throwable)in.readObject();

            if (cause != null)
                initCause(cause);

            int suppressedLen = in.readInt();

            for (int i = 0; i < suppressedLen; i++)
                addSuppressed((Throwable)in.readObject());
        }
    }
}
