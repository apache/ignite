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

package org.apache.ignite.ml.util.generators.dataset;

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.ml.util.generators.function.ParametricVectorGenerator;
import org.apache.ignite.ml.util.generators.variable.DiscreteRandomProducer;
import org.apache.ignite.ml.util.generators.variable.UniformRandomProducer;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

//TODO: builder
public class RandomVectorsGenerator implements Iterator<RandomVectorsGenerator.VectorWithDistributionFamily> {
    private final List<ParametricVectorGenerator> families;
    private final DiscreteRandomProducer familySelector;
    private final UniformRandomProducer paramValuesGenerator;

    public RandomVectorsGenerator(List<ParametricVectorGenerator> families,
        DiscreteRandomProducer familySelector,
        UniformRandomProducer paramValuesGenerator) {

        this.families = families;
        this.familySelector = familySelector;
        this.paramValuesGenerator = paramValuesGenerator;

        checkValues();
    }

    @Override public VectorWithDistributionFamily next() {
        int family = families.size() == 1 ? 0 : familySelector.get().intValue();
        Vector vector = families.get(family).apply(paramValuesGenerator.get());
        return new VectorWithDistributionFamily(vector, family);
    }

    @Override public boolean hasNext() {
        return true;
    }

    private void checkValues() {
        if(familySelector == null && families.size() == 1)
            return;

        if(families.isEmpty())
            throw new IllegalArgumentException("Empty distribution families list");

        long countOfUniqVectorSizes = families.stream().map(x -> x.apply(paramValuesGenerator.get()))
            .mapToInt(Vector::size).distinct().count();
        if(countOfUniqVectorSizes != 1)
            throw new IllegalArgumentException("All families should generate vectors of same size");

        if(families.size() != familySelector.size())
            throw new IllegalArgumentException("Family selector size should equal to families count");
    }

    public static class VectorWithDistributionFamily {
        private final Vector vector;
        private final int distributionFamilyId;

        public VectorWithDistributionFamily(Vector vector, int distributionFamilyId) {
            this.vector = vector;
            this.distributionFamilyId = distributionFamilyId;
        }

        public Vector vector() {
            return vector;
        }

        public int distributionFamilyId() {
            return distributionFamilyId;
        }

        public VectorWithDistributionFamily map(IgniteFunction<Vector, Vector> f) {
            return new VectorWithDistributionFamily(f.apply(vector), distributionFamilyId);
        }
    }
}
