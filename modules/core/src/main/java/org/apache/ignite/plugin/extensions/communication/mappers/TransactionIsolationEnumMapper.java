package org.apache.ignite.plugin.extensions.communication.mappers;

import org.apache.ignite.transactions.TransactionIsolation;

/** */
public class TransactionIsolationEnumMapper implements EnumMapper<TransactionIsolation> {
    /** {@inheritDoc} */
    @Override public byte encode(TransactionIsolation val) {
        switch (val) {
            case READ_COMMITTED:
                return 0;
            case REPEATABLE_READ:
                return 1;
            case SERIALIZABLE:
                return 2;
            default:
                return -1;
        }
    }

    /** {@inheritDoc} */
    @Override public TransactionIsolation decode(byte code) {
        if (code == -1)
            return null;

        switch (code) {
            case 0:
                return TransactionIsolation.READ_COMMITTED;
            case 1:
                return TransactionIsolation.REPEATABLE_READ;
            case 2:
                return TransactionIsolation.SERIALIZABLE;
            default:
                return null;
        }
    }
}
