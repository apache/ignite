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

package org.apache.ignite.schema.model;

import javafx.beans.property.*;
import javafx.beans.value.*;
import javafx.collections.*;
import org.apache.ignite.cache.query.*;

import java.util.*;

/**
 * Descriptor for java type.
 */
public class PojoDescriptor {
    /** Selected property. */
    private final BooleanProperty use;

    /** Key class name to show on screen. */
    private final StringProperty keyClsName;

    /** Previous name for key class. */
    private final String keyClsNamePrev;

    /** Value class name to show on screen. */
    private final StringProperty valClsName;

    /** Previous name for value class. */
    private final String valClsNamePrev;

    /** Parent item (schema name). */
    private final PojoDescriptor parent;

    /** Type metadata. */
    private final CacheQueryTypeMetadata typeMeta;

    /** Children items (tables names). */
    private Collection<PojoDescriptor> children = Collections.emptyList();

    /** Indeterminate state of parent. */
    private final BooleanProperty indeterminate = new SimpleBooleanProperty(false);

    /** Full database name: schema + table. */
    private final String fullDbName;

    /** Java class fields. */
    private final ObservableList<PojoField> fields;

    /**
     * Create special descriptor for database schema.
     *
     * @param schema Database schema name.
     * @return New {@code PojoDescriptor} instance.
     */
    public static PojoDescriptor schema(String schema) {
        CacheQueryTypeMetadata typeMeta = new CacheQueryTypeMetadata();

        typeMeta.setSchema(schema);
        typeMeta.setTableName("");
        typeMeta.setKeyType("");
        typeMeta.setType("");

        return new PojoDescriptor(null, typeMeta, Collections.<PojoField>emptyList());
    }

    /**
     * Constructor of POJO descriptor.
     *
     * @param prn Parent descriptor.
     * @param typeMeta Type metadata descriptor.
     * @param fields List of POJO fields.
     */
    public PojoDescriptor(PojoDescriptor prn, CacheQueryTypeMetadata typeMeta, List<PojoField> fields) {
        parent = prn;

        fullDbName = typeMeta.getSchema() + "." + typeMeta.getTableName();

        keyClsNamePrev = typeMeta.getKeyType();
        keyClsName = new SimpleStringProperty(keyClsNamePrev);

        valClsNamePrev = typeMeta.getType();
        valClsName = new SimpleStringProperty(valClsNamePrev);

        use = new SimpleBooleanProperty(true);

        use.addListener(new ChangeListener<Boolean>() {
            @Override public void changed(ObservableValue<? extends Boolean> val, Boolean oldVal, Boolean newVal) {
                for (PojoDescriptor child : children)
                    child.use.set(newVal);

                if (parent != null && !parent.children.isEmpty()) {
                    Iterator<PojoDescriptor> it = parent.children.iterator();

                    boolean parentIndeterminate = false;
                    boolean first = it.next().use.get();

                    while (it.hasNext()) {
                        if (it.next().use.get() != first) {
                            parentIndeterminate = true;

                            break;
                        }
                    }

                    parent.indeterminate.set(parentIndeterminate);

                    if (!parentIndeterminate)
                        parent.use.set(first);
                }
            }
        });

        this.fields = FXCollections.observableList(fields);

        this.typeMeta = typeMeta;
    }

    /**
     * @return Parent descriptor.
     */
    public PojoDescriptor parent() {
        return parent;
    }

    /**
     * @return Full database name: schema + table.
     */
    public String fullDbName() {
        return fullDbName;
    }

    /**
     * @return {@code true} if POJO descriptor is a table descriptor and selected in GUI.
     */
    public boolean selected() {
        return parent != null && use.get();
    }

    /**
     * @return Boolean property support for {@code use} property.
     */
    public BooleanProperty useProperty() {
        return use;
    }

    /**
     * @return Boolean property support for parent {@code indeterminate} property.
     */
    public BooleanProperty indeterminate() {
        return indeterminate;
    }

    /**
     * @return Key class name.
     */
    public String keyClassName() {
        return keyClsName.get();
    }

    /**
     * @return Value class name.
     */
    public String valueClassName() {
        return valClsName.get();
    }

    /**
     * @return Collection of key fields.
     */
    public Collection<PojoField> keyFields() {
        Collection<PojoField> keys = new ArrayList<>();

        for (PojoField field : fields)
            if (field.key())
                keys.add(field);

        return keys;
    }

    /**
     * @param includeKeys If {@code true} include key fields into list of value fields.
     * @return Collection of value fields.
     */
    public Collection<PojoField> valueFields(boolean includeKeys) {
        if (includeKeys)
            return fields;

        Collection<PojoField> vals = new ArrayList<>();

        for (PojoField field : fields)
            if (!field.key())
                vals.add(field);

        return vals;
    }

    /**
     * @return Key class name property.
     */
    public StringProperty keyClassNameProperty() {
        return keyClsName;
    }

    /**
     * @return Value class name property.
     */
    public StringProperty valueClassNameProperty() {
        return valClsName;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return typeMeta.getSchema();
    }

    /**
     * @return Table name.
     */
    public String table() {
        return typeMeta.getTableName();
    }

    /**
     * Sets children items.
     *
     * @param children Items to set.
     */
    public void children(Collection<PojoDescriptor> children) {
        this.children = children;
    }

    /**
     * @return {@code true} if descriptor was changed by user via GUI.
     */
    public boolean changed() {
        if (!keyClsName.get().equals(keyClsNamePrev) || !valClsName.get().equals(valClsNamePrev))
            return true;

        for (PojoField field : fields)
            if (field.changed())
                return true;

        return false;
    }

    /**
     * Revert changes to java names made by user.
     */
    public void revertJavaNames() {
        for (PojoField field : fields)
            field.resetJavaName();
    }

    /**
     * @return Java class fields.
     */
    public ObservableList<PojoField> fields() {
        return fields;
    }

    /**
     * @param includeKeys {@code true} if key fields should be included into value class.
     * @return Type metadata updated with user changes.
     */
    public CacheQueryTypeMetadata metadata(boolean includeKeys) {
        typeMeta.setKeyType(keyClsName.get());
        typeMeta.setType(valClsName.get());

        Collection<CacheQueryTypeDescriptor> keys = new ArrayList<>();

        Collection<CacheQueryTypeDescriptor> vals = new ArrayList<>();

        for (PojoField field : fields) {
            if (field.key()) {
                keys.add(field.descriptor());

                if (includeKeys)
                    vals.add(field.descriptor());
            }
            else
                vals.add(field.descriptor());
        }

        typeMeta.setKeyDescriptors(keys);

        typeMeta.setValueDescriptors(vals);

        return typeMeta;
    }
}
