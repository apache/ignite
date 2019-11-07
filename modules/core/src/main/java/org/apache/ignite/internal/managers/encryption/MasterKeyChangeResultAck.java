package org.apache.ignite.internal.managers.encryption;

import java.util.UUID;
import org.apache.ignite.internal.util.distributed.FinishMessage;

public class MasterKeyChangeResultAck extends MasterKeyChangeAbstractMessage implements FinishMessage {
    /** Error that caused this change to be rejected. */
    private String err;

    /**
     * @param reqId      Request id.
     */
    protected MasterKeyChangeResultAck(UUID reqId, String err) {
        super(reqId, null, null);

        this.err = err;
    }

    /** @return Error that caused this change to be rejected. */
    public String error() {
        return err;
    }

    /** @return {@code True} if has error. */
    public boolean hasError() {
        return err != null;
    }

    @Override public UUID requestId() {
        return reqId;
    }
}
