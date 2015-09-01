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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.h2.result.Row;
import org.h2.value.Value;

/**
 * Row with locking support needed for unique key conflicts resolution.
 */
public class GridH2Row extends Row implements GridSearchRowPointer {
    /**
     * @param data Column values.
     */
    public GridH2Row(Value... data) {
        super(data, MEMORY_CALCULATE);
    }

    /** {@inheritDoc} */
    @Override public long pointer() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void incrementRefCount() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void decrementRefCount() {
        throw new IllegalStateException();
    }
}