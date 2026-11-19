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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Abstract {@code Sampler} implementation
 */
public abstract class AbstractSampler implements Sampler {
    /** */
    private final SampleFactory sampleFactory;

    /**
     * @param sampleFactory Sample factory.
     */
    protected AbstractSampler(SampleFactory sampleFactory) {
        this.sampleFactory = sampleFactory;
    }

    /** {@inheritDoc} */
    @Override public Object[] sample(EstimationDataModel... dataModels) throws SamplingException {
        if (dataModels == null || dataModels.length == 0)
            return null;

        final Object[] samples = new Object[dataModels.length];

        for (int i = 0; i < samples.length; i++) {
            if (dataModels[i] == null)
                throw new SamplingException("Bad dataModels item found [dataModel#" + i + " = null]");

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
    protected abstract Object sample(EstimationDataModel dataModel) throws SamplingException;

    /**
     * @param sample Sample.
     * @param dataModel Data model.
     * @param parentFieldName Parent field name.
     * @param nullPred Null predicate.
     * @return Sample instance with sampled fields.
     * @throws SamplingException If failed.
     */
    protected Sample sampleFields(
        Sample sample,
        EstimationDataModel dataModel,
        String parentFieldName,
        IgnitePredicate<EstimationDataModel.FieldStats> nullPred) throws SamplingException {

        if (sample.className().startsWith("java."))
            return sample;

        for (Sample.Field field : sample.fields()) {
            if (field.type().isPrimitive())
                continue;

            final String statsFieldName = parentFieldName != null ?
                parentFieldName + "." + field.name() :
                field.name();

            EstimationDataModel.FieldStats stats = null;

            if (dataModel.fieldStatsMap() != null)
                stats = dataModel.fieldStatsMap().get(statsFieldName);

            if (nullPred.apply(stats)) {
                field.value(null);

                continue;
            }

            if (String.class == field.type()) {
                if (stats != null && stats.averageSize() != null)
                    field.value(new String(new char[stats.averageSize()]));
                else if (field.value() == null)
                    throw new SamplingException(
                        "No fieldStat or averageSize for string field '" + statsFieldName
                            + "' in dataModel[className = " + dataModel.className() + "]");
            }

            if (field.type().getName().startsWith("java.lang."))
                continue;

            if (field.type().isArray()) {
                if (stats == null || stats.averageSize() == null) {
                    throw new SamplingException(
                        "No fieldStat or averageSize for array field '" + statsFieldName
                            + "' in dataModel[className = " + dataModel.className() + "]");
                }

                final Class<?> elementType = field.type().getComponentType();

                final Object arrObj = Array.newInstance(elementType, stats.averageSize());

                if (!elementType.isPrimitive()) {
                    final Object[] arr = (Object[])arrObj;

                    for (int i = 0; i < arr.length; i++)
                        arr[i] = sampleFields(createSample(elementType), dataModel, statsFieldName, nullPred).sample();
                }

                field.value(arrObj);

                continue;
            }

            if (field.value() == null)
                field.value(sampleFields(createSample(field.type()), dataModel, statsFieldName, nullPred).sample());
        }
        return sample;
    }

    /**
     * @param dataModel Data model.
     * @return Newly created Sample instance.
     * @throws SamplingException If failed.
     */
    protected Sample createSample(EstimationDataModel dataModel) throws SamplingException {
        return sampleFactory.createSample(dataModel);
    }

    /**
     * @param cls Class.
     * @return Newly created Sample instance.
     * @throws SamplingException If failed.
     */
    protected Sample createSample(Class<?> cls) throws SamplingException {
        return new ReflectionSample(newInstance(cls));
    }

    /**
     * @param cls Class to instantiate.
     * @return New instance of the class. For {@link Number} descendants, constructor with {@link String} argument with
     * value {@code "1"} will used.
     * @throws SamplingException If failed.
     */
    private Object newInstance(Class<?> cls) throws SamplingException {
        try {
            if (Number.class.isAssignableFrom(cls)) {
                Constructor<?> constructor = cls.getConstructor(String.class);

                return constructor.newInstance("1");
            }

            return U.newInstance(cls);
        }
        catch (IgniteCheckedException | ReflectiveOperationException e) {
            throw new SamplingException(e);
        }
    }
}
