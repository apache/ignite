/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
