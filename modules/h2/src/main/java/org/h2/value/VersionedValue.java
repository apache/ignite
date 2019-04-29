/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

/**
 * A versioned value (possibly null).
 * It contains current value and latest committed value if current one is uncommitted.
 * Also for uncommitted values it contains operationId - a combination of
 * transactionId and logId.
 */
public class VersionedValue {

    /**
     * Used when we don't care about a VersionedValue instance.
     */
    public static final VersionedValue DUMMY = new VersionedValue();

    protected VersionedValue() {}

    public boolean isCommitted() {
        return true;
    }

    public long getOperationId() {
        return 0L;
    }

    public Object getCurrentValue() {
        return this;
    }

    public Object getCommittedValue() {
        return this;
    }

}
