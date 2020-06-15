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
package org.apache.ignite.compatibility.sql.runner;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.ignite.compatibility.sql.model.ModelFactory;
import org.apache.ignite.compatibility.sql.randomsql.Operator;
import org.apache.ignite.compatibility.sql.randomsql.Schema;
import org.apache.ignite.compatibility.sql.randomsql.Scope;
import org.apache.ignite.compatibility.sql.randomsql.ast.Select;

/**
 *
 */
public class RandomQuerySupplier implements Supplier<String> {

    /** */
    private final Schema schema;

    /** */
    public RandomQuerySupplier(Iterable<ModelFactory> mdlFactories) {
        schema = new Schema();

        for (ModelFactory factory : mdlFactories)
            schema.addTable(factory.queryEntity());

//        schema.addOperator(new Operator(">=", Integer.class, Integer.class, Boolean.class));
//        schema.addOperator(new Operator("<=", Integer.class, Integer.class, Boolean.class));
        schema.addOperator(new Operator("=", Integer.class, Integer.class, Boolean.class));
        schema.addOperator(new Operator("AND", Boolean.class, Boolean.class, Boolean.class));
//        schema.addOperator(new Operator("OR", Boolean.class, Boolean.class, Boolean.class));
    }

    /** {@inheritDoc} */
    @Override public String get() {
        Scope scope = new Scope(schema, ThreadLocalRandom.current().nextInt());

        Select select = Select.createParentRandom(scope);

        StringBuilder sb = new StringBuilder();

        select.print(sb);

        return sb.toString();
    }
}
