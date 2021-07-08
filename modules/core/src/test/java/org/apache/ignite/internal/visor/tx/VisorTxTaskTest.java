package org.apache.ignite.internal.visor.tx;

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.junit.Assert.*;

/**
 * Unit test for VisorTxTaskTest
 */
public class VisorTxTaskTest extends GridCommonAbstractTest {

    @Test
    public void testResuceWithLimit() {

    }

    private VisorTxInfo createMockTransactionInfo(IgniteUuid xid, IgniteUuid nearXid) {
        return new VisorTxInfo(xid, 0L, 0L, null, null, 0L, null, null, ACTIVE, 0, nearXid, null, null, null);
    }

    private Vi

}
