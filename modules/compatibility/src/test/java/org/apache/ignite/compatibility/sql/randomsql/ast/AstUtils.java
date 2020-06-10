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

import java.util.List;
import java.util.Random;

/**
 * TODO: Add class description.
 */
public class AstUtils {

    public static int r(Ast parent, int bound) {
        return parent.random().nextInt(bound);
    }

    public static String rAsString(Ast parent, int bound) {
        return String.valueOf(parent.random().nextInt(bound));
    }

    public static <T> T pickRandom(List<T> list, Random rnd) {
        return list.get(rnd.nextInt(list.size()));
    }
}
