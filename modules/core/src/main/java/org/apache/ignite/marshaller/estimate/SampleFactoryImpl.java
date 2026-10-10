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

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class SampleFactoryImpl implements SampleFactory {
    /** */
    private final IgniteBinary binary;

    /**
     * @param binary IgniteBinary instance.
     */
    public SampleFactoryImpl(IgniteBinary binary) {
        this.binary = binary;
    }

    /** {@inheritDoc} */
    @Override public Sample createSample(EstimationDataModel dataModel) throws SamplingException {
        try {
            if (dataModel.fieldTypes() == null)
                return new ReflectionSample(U.newInstance(dataModel.className()));
            else
                return new BinarySample(binary, dataModel.className(), dataModel.fieldTypes());
        }
        catch (IgniteCheckedException e) {
            throw new SamplingException(e);
        }
    }
}
