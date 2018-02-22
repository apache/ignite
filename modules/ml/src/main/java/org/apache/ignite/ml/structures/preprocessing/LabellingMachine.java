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

package org.apache.ignite.ml.structures.preprocessing;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.structures.LabeledDataset;

/** Data pre-processing step which assigns labels to all observations according model. */
public class LabellingMachine {
    /**
     * Set labels to each observation according passed model.
     * <p>
     * NOTE: In-place operation.
     * </p>
     * @param ds The given labeled dataset.
     * @param mdl The given model.
     * @return Dataset with predicted labels.
     */
    public static LabeledDataset assignLabels(LabeledDataset ds, Model mdl) {
        for (int i = 0; i < ds.rowSize(); i++) {
            double predictedCls = (double) mdl.apply(ds.getRow(i).features());
            ds.setLabel(i, predictedCls);
        }
        return ds;
    }
}
