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

package org.apache.ignite.internal.processors.metric.list.walker;

import org.apache.ignite.internal.processors.metric.list.view.SqlSchemaView;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;

/** */
public class SqlSchemaViewWalker implements MonitoringRowAttributeWalker<SqlSchemaView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "monitoringRowId", String.class);
        v.accept(1, "monitoringRowId", Object.class);
        v.accept(2, "name", String.class);
        v.acceptBoolean(3, "predefined");
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(SqlSchemaView row, AttributeWithValueVisitor v) {
        v.accept(0, "monitoringRowId", String.class, row.monitoringRowId());
        v.accept(1, "monitoringRowId", Object.class, row.monitoringRowId());
        v.accept(2, "name", String.class, row.name());
        v.acceptBoolean(3, "predefined", row.predefined());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 4;
    }
}

