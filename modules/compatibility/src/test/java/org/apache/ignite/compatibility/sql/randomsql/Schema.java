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
package org.apache.ignite.compatibility.sql.randomsql;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cache.QueryEntity;

/**
 * Schema.
 */
public class Schema {
    /** */
    private final List<Table> tbls = new ArrayList<>();

    /** */
    private final List<Operator> ops = new ArrayList<>();

    /** */
    public void addTable(QueryEntity entity) {
        tbls.add(new Table(entity));
    }

    /** */
    public List<Table> tables() {
        return tbls;
    }

    /** */
    public void fillScope(Scope scope) {
        scope.setSchema(this);
    }

    /** */
    public void addOperator(Operator operator) {
        ops.add(operator);
    }

    /** */
    public List<Operator> operators() {
        return ops;
    }
}
