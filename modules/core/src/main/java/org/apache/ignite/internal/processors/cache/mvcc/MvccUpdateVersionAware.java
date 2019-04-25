/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

/**
 * Interface for objects aware theirs mvcc update version.
 */
public interface MvccUpdateVersionAware {
    /**
     * @return New mvcc coordinator version.
     */
    public long newMvccCoordinatorVersion();

    /**
     * @return New mvcc counter.
     */
    public long newMvccCounter();

    /**
     * @return New mvcc operation counter.
     */
    public int newMvccOperationCounter();

    /**
     * @return New Tx state.
     */
    public byte newMvccTxState();

    /**
     * Copies new MVCC version
     * @param other Object to copy version from.
     */
    public default void newMvccVersion(MvccUpdateVersionAware other) {
        newMvccVersion(other.newMvccCoordinatorVersion(), other.newMvccCounter(), other.newMvccOperationCounter());
    }

    /**
     * Sets new MVCC version
     * @param ver MVCC version.
     */
    public default void newMvccVersion(MvccVersion ver) {
        newMvccVersion(ver.coordinatorVersion(), ver.counter(), ver.operationCounter());
    }

    /**
     * Sets new mvcc version.
     * @param crd New mvcc coordinator version.
     * @param cntr New mvcc counter.
     * @param opCntr New mvcc operation counter.
     */
    public default void newMvccVersion(long crd, long cntr, int opCntr) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return New mvcc version.
     */
    public default MvccVersion newMvccVersion() {
        return new MvccVersionImpl(newMvccCoordinatorVersion(), newMvccCounter(), newMvccOperationCounter());
    }
}
