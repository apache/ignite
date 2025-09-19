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

import org.apache.ignite.lang.IgnitePredicate;

/**
 * {@code X100Sampler} is a {@code Sampler} implementation which makes arrays of 100 sampled instances for each data
 * model considering their field stats.
 */
public class X100Sampler extends AbstractSampler {
    /**
     * @param sampleFactory Sample factory.
     */
    public X100Sampler(SampleFactory sampleFactory) {
        super(sampleFactory);
    }

    /** {@inheritDoc} */
    @Override protected Object sample(EstimationDataModel dataModel) throws SamplingException {
        final Object[] samples = new Object[100];

        for (int i = 0; i < samples.length; i++) {
            final int idx = i;

            samples[i] = sampleFields(
                createSample(dataModel),
                dataModel,
                null,
                new IgnitePredicate<EstimationDataModel.FieldStats>() {
                    @Override public boolean apply(EstimationDataModel.FieldStats stats) {
                        return stats != null && stats.nullsPercent() != null && stats.nullsPercent() < idx;
                    }
                }).sample();
        }

        return samples;
    }
}
