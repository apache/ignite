/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.dml;

import java.util.List;
import org.apache.ignite.internal.util.lang.GridPlainClosure;

/**
 * Method to construct new instances of keys and values on SQL MERGE and INSERT,
 * as well as to build new values during UPDATE - a function that takes a row selected from DB
 * and then transforms it into new object.
 */
public interface KeyValueSupplier extends GridPlainClosure<List<?>, Object> {
    // No-op.
}
