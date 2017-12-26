package org.apache.ignite.jdbc.thin;

/**
 *
 */
public class JdbcThinTransactionsClientAutoCommitComplexSelfTest extends JdbcThinTransactionsAbstractComplexSelfTest {
    /** {@inheritDoc} */
    @Override boolean autoCommit() {
        return true;
    }

    /** {@inheritDoc} */
    @Override int nodeIndex() {
        return CLI_IDX;
    }
}
