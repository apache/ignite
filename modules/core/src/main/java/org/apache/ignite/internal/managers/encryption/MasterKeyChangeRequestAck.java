package org.apache.ignite.internal.managers.encryption;

import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.distributed.FinishMessage;

public class MasterKeyChangeRequestAck extends MasterKeyChangeAbstractMessage implements FinishMessage {
    /** Error that caused this change to be rejected. */
    private String err;

    /**
     * @param req Request.
     * @param err Error.
     */
    MasterKeyChangeRequestAck(MasterKeyChangeRequest req, String err) {
        super(req.requestId(), req.encKeyName(), req.digest());

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
