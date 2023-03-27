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

package org.apache.ignite.ml.tree.randomforest.data.impurity;

import java.util.Map;
import org.apache.ignite.ml.dataset.feature.BucketMeta;

/**
 * Implementation of {@link ImpurityHistogramsComputer} for classification task.
 */
public class GiniHistogramsComputer extends ImpurityHistogramsComputer<GiniHistogram> {
    /** Serial version uid. */
    private static final long serialVersionUID = 3672921182944932748L;

    /** Label mapping. */
    private final Map<Double, Integer> lblMapping;

    /**
     * Creates an instance of GiniHistogramsComputer.
     *
     * @param lblMapping Lbl mapping.
     */
    public GiniHistogramsComputer(Map<Double, Integer> lblMapping) {
        this.lblMapping = lblMapping;
    }

    /** {@inheritDoc} */
    @Override protected GiniHistogram createImpurityComputerForFeature(int sampleId, BucketMeta meta) {
        return new GiniHistogram(sampleId, lblMapping, meta);
    }
}
