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

package org.apache.ignite.internal.processors.query;

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Additional properties to execute the query (Stored in {@link QueryContext}).
 */
public final class QueryProperties {
    /** */
    @Nullable String cacheName;

    /** */
    private final boolean keepBinary;

    /** */
    private final boolean isLocal;

    /** */
    public QueryProperties(@Nullable String cacheName, boolean keepBinary, boolean isLocal) {
        this.cacheName = cacheName;
        this.keepBinary = keepBinary;
        this.isLocal = isLocal;
    }

    /** */
    public boolean keepBinary() {
        return keepBinary;
    }

    /** */
    public @Nullable String cacheName() {
        return cacheName;
    }

    /** */
    public boolean isLocal() {
        return isLocal;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryProperties that = (QueryProperties)o;

        return keepBinary == that.keepBinary && isLocal == that.isLocal && Objects.equals(cacheName, that.cacheName);
    }


    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(cacheName, keepBinary, isLocal);
    }
}
