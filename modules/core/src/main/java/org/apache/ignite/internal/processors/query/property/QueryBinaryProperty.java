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

package org.apache.ignite.internal.processors.query.property;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Binary property.
 */
public class QueryBinaryProperty implements GridQueryProperty {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Property name. */
    private String propName;

    /** */
    private String alias;

    /** Parent property. */
    private QueryBinaryProperty parent;

    /** Result class. */
    private Class<?> type;

    /** */
    private volatile int isKeyProp;

    /** Binary field to speed-up deserialization. */
    private volatile BinaryField field;

    /** Flag indicating that we already tried to take a field. */
    private volatile boolean fieldTaken;

    /** Whether user was warned about missing property. */
    private volatile boolean warned;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param propName Property name.
     * @param parent Parent property.
     * @param type Result type.
     * @param key {@code true} if key property, {@code false} otherwise, {@code null}  if unknown.
     * @param alias Field alias.
     */
    public QueryBinaryProperty(GridKernalContext ctx, String propName, QueryBinaryProperty parent,
        Class<?> type, @Nullable Boolean key, String alias) {

        this.ctx = ctx;

        log = ctx.log(QueryBinaryProperty.class);

        this.propName = propName;
        this.alias = F.isEmpty(alias) ? propName : alias;
        this.parent = parent;
        this.type = type;

        if (key != null)
            this.isKeyProp = key ? 1 : -1;
    }

    /** {@inheritDoc} */
    @Override public Object value(Object key, Object val) throws IgniteCheckedException {
        Object obj;

        if (parent != null) {
            obj = parent.value(key, val);

            if (obj == null)
                return null;

            if (!ctx.cacheObjects().isBinaryObject(obj))
                throw new IgniteCheckedException("Non-binary object received as a result of property extraction " +
                    "[parent=" + parent + ", propName=" + propName + ", obj=" + obj + ']');
        }
        else {
            int isKeyProp0 = isKeyProp;

            if (isKeyProp0 == 0) {
                // Key is allowed to be a non-binary object here.
                // We check key before value consistently with ClassProperty.
                if (key instanceof BinaryObject && ((BinaryObject)key).hasField(propName))
                    isKeyProp = isKeyProp0 = 1;
                else if (val instanceof BinaryObject && ((BinaryObject)val).hasField(propName))
                    isKeyProp = isKeyProp0 = -1;
                else {
                    if (!warned) {
                        U.warn(log, "Neither key nor value have property \"" + propName + "\" " +
                            "(is cache indexing configured correctly?)");

                        warned = true;
                    }

                    return null;
                }
            }

            obj = isKeyProp0 == 1 ? key : val;
        }

        if (obj instanceof BinaryObject) {
            BinaryObject obj0 = (BinaryObject) obj;
            return fieldValue(obj0);
        }
        else if (obj instanceof BinaryObjectBuilder) {
            BinaryObjectBuilder obj0 = (BinaryObjectBuilder)obj;

            return obj0.getField(propName);
        }
        else
            throw new IgniteCheckedException("Unexpected binary object class [type=" + obj.getClass() + ']');
    }

    /** {@inheritDoc} */
    @Override public void setValue(Object key, Object val, Object propVal) throws IgniteCheckedException {
        Object obj = key() ? key : val;

        if (obj == null)
            return;

        Object srcObj = obj;

        if (!(srcObj instanceof BinaryObjectBuilder))
            throw new UnsupportedOperationException("Individual properties can be set for binary builders only");

        if (parent != null)
            obj = parent.value(key, val);

        boolean needsBuild = false;

        if (obj instanceof BinaryObjectExImpl) {
            if (parent == null)
                throw new UnsupportedOperationException("Individual properties can be set for binary builders only");

            needsBuild = true;

            obj = ((BinaryObjectExImpl)obj).toBuilder();
        }

        if (!(obj instanceof BinaryObjectBuilder))
            throw new UnsupportedOperationException("Individual properties can be set for binary builders only");

        setValue0((BinaryObjectBuilder) obj, propName, propVal, type());

        if (needsBuild) {
            obj = ((BinaryObjectBuilder) obj).build();

            assert parent != null;

            // And now let's set this newly constructed object to parent
            setValue0((BinaryObjectBuilder) srcObj, parent.propName, obj, obj.getClass());
        }
    }

    /**
     * @param builder Object builder.
     * @param field Field name.
     * @param val Value to set.
     * @param valType Type of {@code val}.
     * @param <T> Value type.
     */
    private <T> void setValue0(BinaryObjectBuilder builder, String field, Object val, Class<T> valType) {
        //noinspection unchecked
        builder.setField(field, (T)val, valType);
    }

    /**
     * Get binary field for the property.
     *
     * @param obj Target object.
     * @return Binary field.
     */
    private BinaryField binaryField(BinaryObject obj) {
        if (ctx.query().skipFieldLookup())
            return null;

        BinaryField field0 = field;

        if (field0 == null && !fieldTaken) {
            BinaryType type = obj instanceof BinaryObjectEx ? ((BinaryObjectEx)obj).rawType() : obj.type();

            if (type != null) {
                field0 = type.field(propName);

                assert field0 != null;

                field = field0;
            }

            fieldTaken = true;
        }

        return field0;
    }

    /**
     * Gets field value for the given binary object.
     *
     * @param obj Binary object.
     * @return Field value.
     */
    @SuppressWarnings("IfMayBeConditional")
    private Object fieldValue(BinaryObject obj) {
        BinaryField field = binaryField(obj);

        if (field != null)
            return field.value(obj);
        else
            return obj.field(propName);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return alias;
    }

    /** {@inheritDoc} */
    @Override public Class<?> type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public boolean key() {
        int isKeyProp0 = isKeyProp;

        if (isKeyProp0 == 0)
            throw new IllegalStateException("Ownership flag not set for binary property. Have you set 'keyFields'" +
                " property of QueryEntity in programmatic or XML configuration?");

        return isKeyProp0 == 1;
    }

    /** {@inheritDoc} */
    @Override public GridQueryProperty parent() {
        return parent;
    }
}
