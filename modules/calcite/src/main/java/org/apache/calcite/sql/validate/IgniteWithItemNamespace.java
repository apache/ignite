/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWithItem;
import org.jetbrains.annotations.Nullable;

/** Makes Calcite's package-private WITH namespace available when registering a rewritten CYCLE item. */
public class IgniteWithItemNamespace extends WithItemNamespace {
    // TODO: https://issues.apache.org/jira/browse/IGNITE-29207 Remove with Ignite's CYCLE registration hook.
    /**
     * @param validator Validator registering the rewritten item.
     * @param item WITH item including generated mark and path columns.
     * @param enclosingNode Enclosing query.
     */
    public IgniteWithItemNamespace(SqlValidatorImpl validator, SqlWithItem item, @Nullable SqlNode enclosingNode) {
        super(validator, item, enclosingNode);
    }
}
