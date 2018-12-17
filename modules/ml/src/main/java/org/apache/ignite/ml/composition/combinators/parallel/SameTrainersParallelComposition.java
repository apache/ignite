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

import java.util.List;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.combinators.sequential.SameTrainersSequentialComposition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.trainers.DatasetTrainer;

public class SameTrainersParallelComposition<I, O, M extends Model<I, O>, L, T extends DatasetTrainer<M, L>>
    extends TrainersParallelComposition<I, O, M, L, List<O>, T, SameModelsParallelComposition<I, O, M>, SameTrainersSequentialComposition<I, O, M, L, T>, List<O>> {
    public SameTrainersParallelComposition(T tr1,
        SameTrainersParallelComposition<I, O, M, L, T> tr2,
        IgniteBiFunction<O, List<O>, List<O>> merger) {
        super(tr1, tr2, merger);
    }

    public SameTrainersParallelComposition(List<T> trainers) {
        super(trainers.get(0),
            new SameTrainersParallelComposition<I, O, M, L, T>(trainers.subList(1)));
    }
}
