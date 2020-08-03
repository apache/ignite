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

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Index column definition.
 */
public class SqlIndexColumn {
    /** Column name. */
    private final String name;

    /** Descending flag. */
    private final boolean desc;

    /**
     * Constructor.
     *
     * @param name Column name.
     * @param desc Descending flag.
     */
    public SqlIndexColumn(String name, boolean desc) {
        this.name = name;
        this.desc = desc;
    }

    /**
     * @return Column name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Descending flag.
     */
    public boolean descending() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlIndexColumn.class, this);
    }
}
