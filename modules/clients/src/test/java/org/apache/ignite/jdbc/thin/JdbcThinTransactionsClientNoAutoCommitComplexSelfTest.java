package org.apache.ignite.jdbc.thin;

/**
 *
 */
public class JdbcThinTransactionsClientNoAutoCommitComplexSelfTest extends JdbcThinTransactionsAbstractComplexSelfTest {
    /** {@inheritDoc} */
    @Override boolean autoCommit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override int nodeIndex() {
        return CLI_IDX;
    }
}
