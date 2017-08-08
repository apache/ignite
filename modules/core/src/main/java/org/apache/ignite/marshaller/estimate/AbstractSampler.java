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

/**
 * Abstract {@code Sampler} implementation
 */
public abstract class AbstractSampler implements Sampler {
    /** {@inheritDoc} */
    @Override public Object[] sample(DataModel... dataModels) throws SamplingException {
        if (dataModels == null || dataModels.length == 0)
            return null;

        final Object[] samples = new Object[dataModels.length];

        for (int i = 0; i < samples.length; i++) {
            if (dataModels[i] == null)
                throw new SamplingException("dataModel #" + i + " is null");

            samples[i] = sample(dataModels[i]);
        }

        return samples;
    }

    /**
     * Samples specified single data model.
     *
     * @param dataModel Data model to sample.
     * @return Sampled object(s) of specified data model.
     * @throws SamplingException when error occurs during sampling process.
     */
    protected abstract Object sample(DataModel dataModel) throws SamplingException;
}
