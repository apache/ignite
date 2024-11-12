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

package org.apache.ignite.internal.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteSession;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.context.ApplicationContextInternal;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteSessionImpl implements IgniteSession {
    /** */
    private final IgniteEx ign;

    /** */
    private final Map<String, String> attrs;

    /** */
    private final boolean keepBinary;

    /** */
    public IgniteSessionImpl(IgniteEx ign, @Nullable Map<String, String> attrs, boolean keepBinary) {
        this.ign = ign;
        this.attrs = attrs == null ? null : new HashMap<>(attrs);
        this.keepBinary = keepBinary;
    }

    /** */
    @Override public IgniteSession withKeepBinary(boolean keepBinary) {
        return new IgniteSessionImpl(ign, attrs, keepBinary);
    }

    /** */
    @Override public IgniteSession withApplicationAttributes(Map<String, String> attrs) {
        return new IgniteSessionImpl(ign, attrs, keepBinary);
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        try (ApplicationContextInternal ignored = ign.context().applicationContext().withApplicationContext(attrs)) {
            return ign.context().query().querySqlFields(qry, keepBinary);
        }
    }
}
