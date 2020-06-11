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
package org.apache.ignite.compatibility.sql.randomsql.ast;

import java.util.Random;
import org.apache.ignite.compatibility.sql.randomsql.Scope;

/**
 * Base class for AST
 */
public abstract class Ast {

    protected final Ast parent;

    protected final Scope scope;

    protected final int level;

    protected final Random rnd;


    public Ast(Ast parent) {
        this.parent = parent;
        level = parent.level + 1;
        scope = parent.scope;
        rnd = parent.rnd;
    }

    /**
     * Constructor for root AST.
     *
     * @param scope Scope.
     * @param seed Random seed.
     */
    public Ast(Scope scope, int seed) {
        parent = null;
        rnd = new Random(seed);
        this.scope = scope;
        this.scope.setRandom(rnd);
        level = 0;
    }

    public abstract void print(StringBuilder out);

    public Random random() {
        return rnd;
    }

    public Scope scope() {
        return scope;
    }
}
