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

package org.apache.ignite.marshaller.estimate;

import org.apache.ignite.marshaller.Marshaller;

/**
 * {@code MarshalledSizeEstimator} allows to estimate size of memory needed for object(s) described in data model(s).
 */
public interface MarshalledSizeEstimator {
    /**
     * Estimates size of data models by specified marshaller.
     *
     * @param marshaller Marshaller implementation.
     * @param dataModels Data models to estimate.
     * @return estimated size of data models.
     * @throws EstimationException when estimation error occurs.
     */
    public long estimate(Marshaller marshaller, EstimationDataModel... dataModels) throws EstimationException;
}
