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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.stat.IoStatisticsHolder;

/**
 */
public interface FreeList<T extends Storable> {
    /**
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    public void insertDataRow(T row, IoStatisticsHolder statHolder) throws IgniteCheckedException;

    /**
     * @param link Row link.
     * @param row New row data.
     * @return {@code True} if was able to update row.
     * @throws IgniteCheckedException If failed.
     */
    public boolean updateDataRow(long link, T row, IoStatisticsHolder statHolder) throws IgniteCheckedException;

    /**
     * @param link Row link.
     * @param pageHnd Page handler.
     * @param arg Handler argument.
     * @param <S> Argument type.
     * @param <R> Result type.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public <S, R> R updateDataRow(long link, PageHandler<S, R> pageHnd, S arg, IoStatisticsHolder statHolder)
        throws IgniteCheckedException;

    /**
     * @param link Row link.
     * @throws IgniteCheckedException If failed.
     */
    public void removeDataRowByLink(long link, IoStatisticsHolder statHolder) throws IgniteCheckedException;

    /**
     * @param log Logger.
     */
    public void dumpStatistics(IgniteLogger log);
}
