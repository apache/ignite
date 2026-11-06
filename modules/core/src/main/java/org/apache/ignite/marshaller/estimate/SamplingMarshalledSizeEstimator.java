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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.marshaller.Marshaller;

/**
 * MarshalledSizeEstimator implementation which uses {@code Sampler} instance for sampling data models.
 */
public class SamplingMarshalledSizeEstimator implements MarshalledSizeEstimator {
    /** */
    private final Sampler sampler;

    /**
     * @param sampler Sampler implementation.
     */
    public SamplingMarshalledSizeEstimator(Sampler sampler) {
        if (sampler == null)
            throw new NullPointerException("sampler");

        this.sampler = sampler;
    }

    /** {@inheritDoc} */
    @Override public long estimate(Marshaller marshaller, EstimationDataModel... dataModels) throws EstimationException {
        final Object[] samples = sampler.sample(dataModels);

        long estimatedSize = 0L;

        if (samples != null && samples.length > 0) {
            for (int i = 0; i < samples.length; i++) {
                final Object sample = samples[i];

                final byte[] marshalledBytes;

                try {
                    marshalledBytes = marshaller.marshal(sample);
                }
                catch (IgniteCheckedException e) {
                    throw new EstimationException(e);
                }

                long sampleEstimatedSize = marshalledBytes.length * dataModels[i].count();

                if (sample.getClass().isArray()) {
                    Object[] sampleArray = (Object[]) sample;

                    sampleEstimatedSize /= sampleArray.length;
                }

                estimatedSize += sampleEstimatedSize;
            }
        }

        return estimatedSize;
    }
}
