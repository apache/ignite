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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;

/**
 */
public interface FreeList<T extends Storable> {
    /**
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    public void insertDataRow(T row) throws IgniteCheckedException;

    /**
     * @param link Row link.
     * @param row New row data.
     * @return {@code True} if was able to update row.
     * @throws IgniteCheckedException If failed.
     */
    public boolean updateDataRow(long link, T row) throws IgniteCheckedException;

    /**
     * @param link Row link.
     * @param pageHnd Page handler.
     * @param arg Handler argument.
     * @param <S> Argument type.
     * @param <R> Result type.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public <S, R>  R updateDataRow(long link, PageHandler<S, R> pageHnd, S arg) throws IgniteCheckedException;

    /**
     * @param link Row link.
     * @throws IgniteCheckedException If failed.
     */
    public void removeDataRowByLink(long link) throws IgniteCheckedException;

    /**
     * @param log Logger.
     */
    public void dumpStatistics(IgniteLogger log);
}
