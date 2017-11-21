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

package org.apache.ignite.ml;

import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer;

/**
 * Interface for Trainers. Trainer is just a function which produces model from the data.
 * See for example {@link ColumnDecisionTreeTrainer}.
 * @param <M> Type of produced model.
 * @param <T> Type of data needed for model producing.
 */
public interface Trainer<M extends Model, T> {
    /**
     * Returns model based on data
     * @param data data to build model
     * @return model
     */
    M train(T data);
}
