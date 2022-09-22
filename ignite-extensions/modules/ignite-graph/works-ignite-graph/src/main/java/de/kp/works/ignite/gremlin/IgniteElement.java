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

import de.kp.works.ignite.IgniteTable;
import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.gremlin.models.ElementModel;
import de.kp.works.ignite.gremlin.mutators.Mutator;
import de.kp.works.ignite.gremlin.mutators.Mutators;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class IgniteElement implements Element {

    protected IgniteGraph graph;
    protected final Object id;
    protected String label;
    protected Long createdAt;
    protected Long updatedAt;
    protected Map<String, Object> properties;
    protected transient boolean propertiesFullyLoaded;
    protected transient IndexMetadata.Key indexKey;
    protected transient long indexTs;
    protected transient boolean isCached;
    protected transient boolean isDeleted;

    protected IgniteElement(IgniteGraph graph,
                            Object id,
                            String label,
                            Long createdAt,
                            Long updatedAt,
                            Map<String, Object> properties,
                            boolean propertiesFullyLoaded) {
        this.graph = graph;
        this.id = id;
        this.label = label;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.properties = properties;
        this.propertiesFullyLoaded = propertiesFullyLoaded;
    }

    public abstract void validate();

    public abstract ElementType getElementType();

    public IgniteTable getTable() {
        return getModel().getTable();
    }

    @Override
    public Graph graph() {
        return graph;
    }

    public void setGraph(IgniteGraph graph) {
        this.graph = graph;
    }

    @Override
    public Object id() {
        return id;
    }

    public IndexMetadata.Key getIndexKey() {
        return indexKey;
    }

    public void setIndexKey(IndexMetadata.Key indexKey) {
        this.indexKey = indexKey;
    }

    public long getIndexTs() {
        return indexTs;
    }

    public void setIndexTs(long indexTs) {
        this.indexTs = indexTs;
    }

    public boolean isCached() {
        return isCached;
    }

    public void setCached(boolean isCached) {
        this.isCached = isCached;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean isDeleted) {
        this.isDeleted = isDeleted;
    }

    public Map<String, Object> getProperties() {
        if (properties == null || !propertiesFullyLoaded) {
            load();
            propertiesFullyLoaded = true;
        }
        return properties;
    }

    public boolean arePropertiesFullyLoaded() {
        return propertiesFullyLoaded;
    }

    public void copyFrom(IgniteElement element) {
        if (element.label != null) this.label = element.label;
        if (element.createdAt != null) this.createdAt = element.createdAt;
        if (element.updatedAt != null) this.updatedAt = element.updatedAt;
        if (element.properties != null
                && (element.propertiesFullyLoaded || this.properties == null)) {
            this.properties = new ConcurrentHashMap<>(element.properties);
            this.propertiesFullyLoaded = element.propertiesFullyLoaded;
        }
    }

    public void load() {
        getModel().load(this);
    }

    @SuppressWarnings("unchecked")
    public <V> V getProperty(String key) {
        if (properties != null) {
            // optimization for partially loaded properties
            V val = (V) properties.get(key);
            if (val != null) return val;
        }
        return (V) getProperties().get(key);
    }

    public boolean hasProperty(String key) {
        if (properties != null) {
            // optimization for partially loaded properties
            Object val = properties.get(key);
            if (val != null) return true;
        }
        return keys().contains(key);
    }

    @Override
    public Set<String> keys() {
        return getPropertyKeys();
    }

    public Set<String> getPropertyKeys() {
        return new HashSet<>(getProperties().keySet());
    }

    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(key, value);

        getProperties().put(key, value);
        updatedAt(System.currentTimeMillis());

        ElementType elementType = getElementType();

        Mutator writer = getModel().writeProperty(this, elementType, key, value);
        Mutators.write(getTable(), writer);
    }

    public void incrementProperty(String key, long value) {

        ElementHelper.validateProperty(key, value);
        updatedAt(System.currentTimeMillis());

        ElementType elementType = getElementType();

        Mutator writer = getModel().incrementProperty(this, elementType, key, value);
        long newValue = Mutators.increment(getTable(), writer, key);
        getProperties().put(key, newValue);
    }

    public <V> V removeProperty(String key) {
        V value = getProperty(key);
        if (value != null) {
            getProperties().remove(key);
            updatedAt(System.currentTimeMillis());

            ElementType elementType = getElementType();

            Mutator writer = getModel().clearProperty(this, elementType, key);
            Mutators.write(getTable(), writer);
        }
        return value;
    }

    @Override
    public String label() {
        if (label == null) load();
        return label;
    }

    public Long createdAt() {
        if (createdAt == null) load();
        return createdAt;
    }

    public Long updatedAt() {
        if (updatedAt == null) load();
        return updatedAt;
    }

    public void updatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public abstract ElementModel getModel();

    public abstract void writeToModel();

    public abstract void deleteFromModel();

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}
