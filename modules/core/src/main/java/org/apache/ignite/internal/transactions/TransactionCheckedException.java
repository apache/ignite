package org.apache.ignite.internal.transactions;

import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Marker of transactional runtime exceptions.
 *
 * {@link IgniteTxHeuristicCheckedException} If operation performs within transaction that entered an unknown state.
 * {@link IgniteTxOptimisticCheckedException} if operation with optimistic behavior failed.
 * {@link IgniteTxRollbackCheckedException} If operation performs within transaction that automatically rolled back.
 * {@link IgniteTxTimeoutCheckedException} If operation performs within transaction and timeout occurred.
 */
public class TransactionCheckedException  extends IgniteCheckedException {
	/** Serial version UID. */
	private static final long serialVersionUID = 0L;

	public TransactionCheckedException() {}

	public TransactionCheckedException(String msg) {
		super(msg);
	}

	public TransactionCheckedException(Throwable cause) {
		super(cause);
	}

	public TransactionCheckedException(String msg, @Nullable Throwable cause) {
		super(msg, cause);
	}
}