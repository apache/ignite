package de.kp.works.ignite.gremlin;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;

public class IgniteGraphFeatures implements Graph.Features {

    protected final boolean supportsPersistence;
    protected final GraphFeatures graphFeatures = new IgniteGraphGraphFeatures();
    protected final VertexFeatures vertexFeatures = new IgniteVertexFeatures();
    protected final EdgeFeatures edgeFeatures = new IgniteEdgeFeatures();

    public IgniteGraphFeatures(boolean supportsPersistence) {
        this.supportsPersistence = supportsPersistence;
    }

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public class IgniteGraphGraphFeatures implements GraphFeatures {

        private final VariableFeatures variableFeatures = new IgniteVariableFeatures();

        IgniteGraphGraphFeatures() {
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return true;
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return supportsPersistence;
        }

        @Override
        public VariableFeatures variables() {
            return variableFeatures;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    public static class IgniteVertexFeatures extends IgniteElementFeatures implements VertexFeatures {

        private final VertexPropertyFeatures vertexPropertyFeatures = new IgniteVertexPropertyFeatures();

        IgniteVertexFeatures() {
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }

        @Override
        public VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsMetaProperties() {
            return false;
        }

        @Override
        public boolean supportsMultiProperties() {
            return false;
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }
    }

    public static class IgniteEdgeFeatures extends IgniteElementFeatures implements EdgeFeatures {

        private final EdgePropertyFeatures edgePropertyFeatures = new IgniteEdgePropertyFeatures();

        IgniteEdgeFeatures() {
        }

        @Override
        public EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }

    }

    public static class IgniteElementFeatures implements ElementFeatures {

        IgniteElementFeatures() {
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return true;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() {
            return true;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return id instanceof Serializable;
        }
    }

    public static class IgniteVertexPropertyFeatures implements VertexPropertyFeatures {

        IgniteVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsMapValues() {
            return true;
        }

        @Override
        public boolean supportsMixedListValues() {
            return true;
        }

        @Override
        public boolean supportsSerializableValues() {
            return true;
        }

        @Override
        public boolean supportsUniformListValues() {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public static class IgniteEdgePropertyFeatures implements EdgePropertyFeatures {

        IgniteEdgePropertyFeatures() {
        }

        @Override
        public boolean supportsMapValues() {
            return true;
        }

        @Override
        public boolean supportsMixedListValues() {
            return true;
        }

        @Override
        public boolean supportsSerializableValues() {
            return true;
        }

        @Override
        public boolean supportsUniformListValues() {
            return true;
        }

    }

    public static class IgniteVariableFeatures implements Graph.Features.VariableFeatures {

        @Override
        public boolean supportsVariables() {
            return false;
        }

        @Override
        public boolean supportsBooleanValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleValues() {
            return false;
        }

        @Override
        public boolean supportsFloatValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerValues() {
            return false;
        }

        @Override
        public boolean supportsLongValues() {
            return false;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsByteValues() {
            return false;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsStringValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
    }
}
