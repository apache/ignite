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

package org.apache.ignite.internal.processors.query.calcite.schema;

/** Technical cache table columns. */
public final class TechnicalColumns {
    /** Field name for row version. */
    public static final String VER_FIELD_NAME = "_VER";

    /** Field name for row source. */
    public static final String SRC_FIELD_NAME = "_SRC";

    /** */
    private TechnicalColumns() {
        // No-op.
    }

    /** */
    public static boolean isTechnicalFieldName(String fieldName) {
        return VER_FIELD_NAME.equals(fieldName) || SRC_FIELD_NAME.equals(fieldName);
    }

    /** */
    public static boolean isTechnicalFieldNameIgnoreCase(String fieldName) {
        return VER_FIELD_NAME.equalsIgnoreCase(fieldName) || SRC_FIELD_NAME.equalsIgnoreCase(fieldName);
    }
}