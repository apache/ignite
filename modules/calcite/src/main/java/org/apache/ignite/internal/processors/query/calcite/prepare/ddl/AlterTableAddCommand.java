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
import java.util.List;

/**
 * ALTER TABLE ... ADD COLUMN statement.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class AlterTableAddCommand extends AbstractAlterTableCommand {
    /** Quietly ignore this command if column already exists. */
    private boolean ifColumnNotExists;

    /** Columns. */
    private List<ColumnDefinition> cols;

    /**
     * @return Columns.
     */
    public List<ColumnDefinition> columns() {
        return Collections.unmodifiableList(cols);
    }

    /**
     * @param cols Columns.
     */
    public void columns(List<ColumnDefinition> cols) {
        this.cols = cols;
    }

    /**
     * @return Quietly ignore this command if column already exists.
     */
    public boolean ifColumnNotExists() {
        return ifColumnNotExists;
    }

    /**
     * @param ifColumnNotExists Quietly ignore this command if column already exists.
     */
    public void ifColumnNotExists(boolean ifColumnNotExists) {
        this.ifColumnNotExists = ifColumnNotExists;
    }
}
