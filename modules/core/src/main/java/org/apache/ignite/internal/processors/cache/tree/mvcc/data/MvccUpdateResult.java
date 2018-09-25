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

package org.apache.ignite.internal.processors.cache.tree.mvcc.data;

import java.util.List;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;

/**
 *
 */
public interface MvccUpdateResult {
    /**
     * @return Type of result.
     */
    public ResultType resultType();

    /**
     * @return Result version.
     */
    public MvccVersion resultVersion();

    /**
     *
     * @return Collection of row created or affected by the current tx.
     */
    public List<MvccLinkAwareSearchRow> history();

    /**
     * @return {@code True} if this key was inserted in the cache with this row in the same transaction.
     */
    public boolean isKeyAbsentBefore();

    /**
     * @return Flag whether tx has overridden it's own update.
     */
    public boolean isOwnValueOverridden();
}
