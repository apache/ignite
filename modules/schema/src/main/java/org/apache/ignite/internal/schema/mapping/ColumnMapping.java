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

package org.apache.ignite.internal.schema.mapping;

import java.io.Serializable;

/**
 * Column mapping helper.
 */
public class ColumnMapping {
    /** Identity mapper. */
    private static final IdentityMapper IDENTITY_MAPPER = new IdentityMapper();

    /**
     * @return Identity mapper instance.
     */
    public static ColumnMapper identityMapping() {
        return IDENTITY_MAPPER;
    }

    /**
     * @param cols Number of columns.
     */
    public static ColumnMapperBuilder mapperBuilder(int cols) {
        return new ColumnMapperImpl(cols);
    }

    /**
     * Stub.
     */
    private ColumnMapping() {
    }

    /**
     * Identity column mapper.
     */
    private static class IdentityMapper implements ColumnMapper, Serializable {
        /** {@inheritDoc} */
        @Override public int map(int idx) {
            return idx;
        }
    }
}
