/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.transactions;

import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Marker of transactional runtime exceptions.
 *
 * {@link TransactionDeadlockException} If operation has been timed out.
 * {@link TransactionHeuristicException} If operation performs within transaction that entered an unknown state.
 * {@link TransactionOptimisticException} if operation with optimistic behavior failed.
 * {@link TransactionRollbackException} If operation performs within transaction that automatically rolled back.
 * {@link TransactionTimeoutException} If operation performs within transaction and timeout occurred.
 */
public class TransactionException extends IgniteException {
	/** Serial version UID. */
	private static final long serialVersionUID = 0L;

	public TransactionException() {}

	public TransactionException(String msg) {
		super(msg);
	}

	public TransactionException(Throwable cause) {
		super(cause);
	}

	public TransactionException(String msg, @Nullable Throwable cause) {
		super(msg, cause);
	}
}