package org.apache.ignite.jdbc.thin;

/**
 *
 */
public class JdbcThinTransactionsServerAutoCommitComplexSelfTest extends JdbcThinTransactionsAbstractComplexSelfTest {
    /** {@inheritDoc} */
    @Override boolean autoCommit() {
        return true;
    }

    /** {@inheritDoc} */
    @Override int nodeIndex() {
        return 0;
    }
}
