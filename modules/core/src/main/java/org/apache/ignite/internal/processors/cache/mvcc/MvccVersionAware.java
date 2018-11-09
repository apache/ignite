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

package org.apache.ignite.internal.processors.cache.mvcc;

/**
 *
 */
public interface MvccVersionAware {
    /**
     * @return Mvcc coordinator version.
     */
    public long mvccCoordinatorVersion();

    /**
     * @return Mvcc counter.
     */
    public long mvccCounter();

    /**
     * @return Mvcc operation counter.
     */
    public int mvccOperationCounter();

    /**
     * @return Tx state hint for 'created' mvcc version.
     */
    public byte mvccTxState();

    /**
     * Copies mvcc version from another object.
     * @param other Info source.
     */
    public default void mvccVersion(MvccVersionAware other) {
        mvccVersion(other.mvccCoordinatorVersion(), other.mvccCounter(), other.mvccOperationCounter());
    }

    /**
     * Sets mvcc version.
     * @param ver Mvcc version.
     */
    public default void mvccVersion(MvccVersion ver) {
        mvccVersion(ver.coordinatorVersion(), ver.counter(), ver.operationCounter());
    }

    /**
     * Sets mvcc version.
     * @param crd Mvcc coordinator version.
     * @param cntr Mvcc counter.
     * @param opCntr Mvcc operation counter.
     */
    public default void mvccVersion(long crd, long cntr, int opCntr) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return Mvcc version.
     */
    public default MvccVersion mvccVersion() {
        return new MvccVersionImpl(mvccCoordinatorVersion(), mvccCounter(), mvccOperationCounter());
    }
}
