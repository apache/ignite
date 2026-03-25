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
package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.processors.query.calcite.message.QueryTxEntry;
import org.jetbrains.annotations.Nullable;

/** Per thread modified entries holder. */
public class TxAwareModifiedEntriesHolder {
    /** Transaction modified entries holder. */
    @Nullable private final ThreadLocal<Collection<QueryTxEntry>> holder;

    /** */
    public TxAwareModifiedEntriesHolder(boolean txAware) {
        if (txAware)
            holder = new ThreadLocal<>();
        else
            holder = null;
    }

    /** Store entries if applicable. */
    public void store(Collection<QueryTxEntry> items) {
        if (holder != null)
            holder.set(items);
    }

    /** Retirieve entries if applicable. */
    public Collection<QueryTxEntry> retrieve() {
        if (holder != null)
            return holder.get();
        else
            return Collections.emptyList();
    }

    /** Detach stored entries. */
    public void detach() {
        if (holder != null)
            holder.remove();
    }
}
