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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.ArrayList;
import java.util.Collections;

/**
 * SQL Array: (1, 2, ?, 'abc')
 */
public class GridSqlValueRow extends GridSqlElement {
    /**
     * @param size Array size.
     */
    public GridSqlValueRow(int size) {
        super(size == 0 ? Collections.emptyList() : new ArrayList<>(size));
    }

    /** {@inheritDoc}  */
    @Override public String getSQL() {
        if (size() == 0)
            return "ROW ()";

        StringBuilder buff = new StringBuilder("ROW (");

        for (int i = 0; i < size(); i++) {
            if (i > 0)
                buff.append(", ");

            buff.append(child(i).getSQL());
        }

        return buff.append(')').toString();
    }
}
