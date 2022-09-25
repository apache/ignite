package de.kp.works.ignite.gremlin;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public final class IgniteVertexProperty<V> implements VertexProperty<V>,java.io.Serializable {
    protected final IgniteVertex vertex;
    protected final String key;
    protected final V value;

    public IgniteVertexProperty(final IgniteVertex vertex, final String key, final V value) {      
        this.vertex = vertex;
        this.key = key;
        this.value = value;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public Object id() {
        return (long) (this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode());
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove() {
        vertex.removeProperty(this.key);
    }

    @Override
    public Set<String> keys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}