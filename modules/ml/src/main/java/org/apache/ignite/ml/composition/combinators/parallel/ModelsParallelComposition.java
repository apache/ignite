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

package org.apache.ignite.ml.composition.combinators.parallel;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

public class ModelsParallelComposition<I, O1, M1 extends Model<I, O1>, O2, M2 extends Model<I, O2>, O3>
    implements Model<I, O3> {
    private final IgniteBiFunction<O1, O2, O3> merger;
    private M1 mdl1;
    private M2 mdl2;

    public ModelsParallelComposition(M1 mdl1, M2 mdl2, IgniteBiFunction<O1, O2, O3> merger) {
        this.mdl1 = mdl1;
        this.mdl2 = mdl2;
        this.merger = merger;
    }

    @Override public O3 apply(I i) {
        O1 res1 = mdl1.apply(i);
        O2 res2 = mdl2.apply(i);

        return merger.apply(res1, res2);
    }
}
