package org.apache.ignite.internal.management.checkpoint;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Checkpoint force command arguments. */
public class CheckpointForceCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(description = "Reason for checkpoint", optional = true)
    private String reason;

    /** */
    @Argument(description = "Wait for checkpoint to finish", optional = true)
    private boolean waitForFinish;

    /** */
    @Argument(description = "Timeout in milliseconds", optional = true)
    private Long timeout;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, reason);
        out.writeBoolean(waitForFinish);
        out.writeObject(timeout);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        reason = U.readString(in);
        waitForFinish = in.readBoolean();
        timeout = (Long)in.readObject();
    }

    /** */
    public String reason() {
        return reason;
    }

    /** */
    public void reason(String reason) {
        this.reason = reason;
    }

    /** */
    public boolean waitForFinish() {
        return waitForFinish;
    }

    /** */
    public void waitForFinish(boolean waitForFinish) {
        this.waitForFinish = waitForFinish;
    }

    /** */
    public Long timeout() {
        return timeout;
    }

    /** */
    public void timeout(Long timeout) {
        this.timeout = timeout;
    }
}