/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.mvstore.MVMap;
import org.h2.value.VersionedValue;

/**
 * Class CommitDecisionMaker makes a decision during post-commit processing
 * about how to transform uncommitted map entry into committed one,
 * based on undo log information.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
final class CommitDecisionMaker extends MVMap.DecisionMaker<VersionedValue> {
    private long undoKey;
    private MVMap.Decision decision;

    void setUndoKey(long undoKey) {
        this.undoKey = undoKey;
        reset();
    }

    @Override
    public MVMap.Decision decide(VersionedValue existingValue, VersionedValue providedValue) {
        assert decision == null;
        if (existingValue == null ||
            // map entry was treated as already committed, and then
            // it has been removed by another transaction (committed and closed by now)
            existingValue.getOperationId() != undoKey) {
            // this is not a final undo log entry for this key,
            // or map entry was treated as already committed and then
            // overwritten by another transaction
            // see TxDecisionMaker.decide()

            decision = MVMap.Decision.ABORT;
        } else /* this is final undo log entry for this key */ if (existingValue.getCurrentValue() == null) {
            decision = MVMap.Decision.REMOVE;
        } else {
            decision = MVMap.Decision.PUT;
        }
        return decision;
    }

    @SuppressWarnings("unchecked")
    @Override
    public VersionedValue selectValue(VersionedValue existingValue, VersionedValue providedValue) {
        assert decision == MVMap.Decision.PUT;
        assert existingValue != null;
        return VersionedValueCommitted.getInstance(existingValue.getCurrentValue());
    }

    @Override
    public void reset() {
        decision = null;
    }

    @Override
    public String toString() {
        return "commit " + TransactionStore.getTransactionId(undoKey);
    }
}
