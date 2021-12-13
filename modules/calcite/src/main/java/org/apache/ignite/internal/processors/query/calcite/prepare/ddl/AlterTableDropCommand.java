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

package org.apache.ignite.internal.processors.query.calcite.prepare.ddl;

import java.util.Collections;
import java.util.Set;

/**
 * ALTER TABLE ... DROP COLUMN statement.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class AlterTableDropCommand extends AbstractTableDdlCommand {
    /** Quietly ignore this command if column is not exists. */
    private boolean ifColumnExists;

    /** Columns. */
    private Set<String> cols;

    public Set<String> columns() {
        return Collections.unmodifiableSet(cols);
    }

    /**
     * Columns to drop.
     *
     * @param cols Columns.
     */
    public void columns(Set<String> cols) {
        this.cols = cols;
    }

    /**
     * Exists flag.
     *
     * @return Quietly ignore this command if column is not exists.
     */
    public boolean ifColumnExists() {
        return ifColumnExists;
    }

    /**
     * Set exists flag.
     *
     * @param ifColumnExists Quietly ignore this command if column is not exists.
     */
    public void ifColumnExists(boolean ifColumnExists) {
        this.ifColumnExists = ifColumnExists;
    }
}
