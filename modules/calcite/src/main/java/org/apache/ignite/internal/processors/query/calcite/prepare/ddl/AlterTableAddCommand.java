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

import java.util.List;

/**
 * ALTER TABLE ... ADD COLUMN statement.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class AlterTableAddCommand extends AbstractTableDdlCommand {
    /** Quietly ignore this command if column already exists. */
    private boolean ifColumnNotExists;

    /** Columns. */
    private List<ColumnDefinition> cols;

    public List<ColumnDefinition> columns() {
        return cols;
    }

    public void columns(List<ColumnDefinition> cols) {
        this.cols = cols;
    }

    public boolean ifColumnNotExists() {
        return ifColumnNotExists;
    }

    public void ifColumnNotExists(boolean ifColumnNotExists) {
        this.ifColumnNotExists = ifColumnNotExists;
    }
}
