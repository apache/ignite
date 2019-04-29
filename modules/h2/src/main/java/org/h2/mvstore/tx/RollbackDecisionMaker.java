/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.mvstore.MVMap;
import org.h2.value.VersionedValue;

/**
 * Class RollbackDecisionMaker process undo log record during transaction rollback.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
final class RollbackDecisionMaker extends MVMap.DecisionMaker<Object[]> {
    private final TransactionStore store;
    private final long transactionId;
    private final long toLogId;
    private final TransactionStore.RollbackListener listener;
    private MVMap.Decision decision;

    RollbackDecisionMaker(TransactionStore store, long transactionId, long toLogId,
                            TransactionStore.RollbackListener listener) {
        this.store = store;
        this.transactionId = transactionId;
        this.toLogId = toLogId;
        this.listener = listener;
    }

    @Override
    public MVMap.Decision decide(Object[] existingValue, Object[] providedValue) {
        assert decision == null;
        if (existingValue == null) {
            // normally existingValue will always be there except of db initialization
            // where some undo log entry was captured on disk but actual map entry was not
            decision = MVMap.Decision.ABORT;
        } else {
            VersionedValue valueToRestore = (VersionedValue) existingValue[2];
            long operationId;
            if (valueToRestore == null ||
                    (operationId = valueToRestore.getOperationId()) == 0 ||
                    TransactionStore.getTransactionId(operationId) == transactionId
                            && TransactionStore.getLogId(operationId) < toLogId) {
                int mapId = (Integer) existingValue[0];
                MVMap<Object, VersionedValue> map = store.openMap(mapId);
                if (map != null && !map.isClosed()) {
                    Object key = existingValue[1];
                    VersionedValue previousValue = map.operate(key, valueToRestore, MVMap.DecisionMaker.DEFAULT);
                    listener.onRollback(map, key, previousValue, valueToRestore);
                }
            }
            decision = MVMap.Decision.REMOVE;
        }
        return decision;
    }

    @Override
    public void reset() {
        decision = null;
    }

    @Override
    public String toString() {
        return "rollback-" + transactionId;
    }
}
