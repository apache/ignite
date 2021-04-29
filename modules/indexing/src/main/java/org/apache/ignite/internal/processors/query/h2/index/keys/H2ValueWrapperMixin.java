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

package org.apache.ignite.internal.processors.query.h2.index.keys;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.h2.value.Value;

/**
 * Mixin provides a method to wrap an object to H2 Value with correct exception handling.
 */
interface H2ValueWrapperMixin {
    /** */
    public default Value wrapToValue(Object obj, int type) {
        try {
            return H2Utils.wrap(null, obj, type);

        } catch (ClassCastException e) {
            throw new IgniteSQLException("Failed to wrap object into H2 Value. " + e.getMessage(),
                IgniteQueryErrorCode.FIELD_TYPE_MISMATCH, e);

        } catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wrap object into H2 Value.", e);
        }
    }
}
