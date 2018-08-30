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

package org.apache.ignite.internal.processors.cache.tree;

/**
 *
 */
public interface RowLinkIO {
    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Row link.
     */
    public long getLink(long pageAddr, int idx);

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Key hash code.
     */
    public int getHash(long pageAddr, int idx);

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Cache ID or {@code 0} if cache ID is not defined.
     */
    default int getCacheId(long pageAddr, int idx) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Mvcc coordinator version.
     */
    default long getMvccCoordinatorVersion(long pageAddr, int idx) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Mvcc operation counter.
     */
    default int getMvccOperationCounter(long pageAddr, int idx) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Mvcc counter.
     */
    default long getMvccCounter(long pageAddr, int idx) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Mvcc coordinator version.
     */
    default long getMvccLockCoordinatorVersion(long pageAddr, int idx) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Mvcc counter.
     */
    default long getMvccLockCounter(long pageAddr, int idx) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @param lockCrd Mvcc lock coordinator version.
     */
    default void setMvccLockCoordinatorVersion(long pageAddr, int idx, long lockCrd) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @param lockCntr Mvcc lock counter.
     */
    default void setMvccLockCounter(long pageAddr, int idx, long lockCntr) {
        throw new UnsupportedOperationException();
    }
}
