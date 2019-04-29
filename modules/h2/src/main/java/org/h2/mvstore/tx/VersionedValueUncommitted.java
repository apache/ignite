/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.value.VersionedValue;

/**
 * Class VersionedValueUncommitted.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
class VersionedValueUncommitted extends VersionedValueCommitted {
    private final long operationId;
    private final Object committedValue;

    private VersionedValueUncommitted(long operationId, Object value, Object committedValue) {
        super(value);
        assert operationId != 0;
        this.operationId = operationId;
        this.committedValue = committedValue;
    }

    /**
     * Create new VersionedValueUncommitted.
     *
     * @param operationId combined log/transaction id
     * @param value value before commit
     * @param committedValue value after commit
     * @return VersionedValue instance
     */
    static VersionedValue getInstance(long operationId, Object value, Object committedValue) {
        return new VersionedValueUncommitted(operationId, value, committedValue);
    }

    @Override
    public boolean isCommitted() {
        return false;
    }

    @Override
    public long getOperationId() {
        return operationId;
    }

    @Override
    public Object getCommittedValue() {
        return committedValue;
    }

    @Override
    public String toString() {
        return super.toString() +
                " " + TransactionStore.getTransactionId(operationId) + "/" +
                TransactionStore.getLogId(operationId) + " " + committedValue;
    }
}
