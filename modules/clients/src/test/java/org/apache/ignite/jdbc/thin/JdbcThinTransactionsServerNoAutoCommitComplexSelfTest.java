package org.apache.ignite.jdbc.thin;

/**
 *
 */
public class JdbcThinTransactionsServerNoAutoCommitComplexSelfTest extends JdbcThinTransactionsAbstractComplexSelfTest {
    /** {@inheritDoc} */
    @Override boolean autoCommit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override int nodeIndex() {
        return 0;
    }
}
