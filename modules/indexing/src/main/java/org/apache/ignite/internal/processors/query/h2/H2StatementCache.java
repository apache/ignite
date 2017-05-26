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
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Statement cache.
 */
public final class H2StatementCache extends LinkedHashMap<String, PreparedStatement> {
    /** */
    private final int maxSize;

    /**
     * @param maxSize Size.
     */
    H2StatementCache(int maxSize) {
        super(Math.min(maxSize, 32), 0.75f, true);

        this.maxSize = maxSize;
    }

    /** {@inheritDoc} */
    @Override protected boolean removeEldestEntry(Map.Entry<String, PreparedStatement> eldest) {
        if (size() <= maxSize)
            return false;

        U.closeQuiet(eldest.getValue());

        return true;
    }
}
