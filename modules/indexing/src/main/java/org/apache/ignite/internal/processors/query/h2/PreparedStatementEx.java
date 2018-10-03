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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface PreparedStatementEx extends PreparedStatement {
    /** */
    static final AtomicInteger metaIdGenerator = new AtomicInteger();

    /** Flag if at least one MVCC cache is used in this statement. */
    static final int MVCC_STATE = metaIdGenerator.getAndIncrement();

    /** First mvcc cache id of the involved caches. */
    static final int MVCC_CACHE_ID = metaIdGenerator.getAndIncrement();

    /**
     * @param id Metadata key.
     * @return Attached metadata.
     */
    @Nullable <T> T meta(int id);

    /**
     * @param id Metadata key.
     * @param metaObj  Metadata object.
     */
    void putMeta(int id, Object metaObj);
}
