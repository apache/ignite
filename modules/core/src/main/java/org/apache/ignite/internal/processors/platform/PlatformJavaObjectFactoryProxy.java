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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.platform.PlatformJavaObjectFactory;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper for Java object factory.
 */
public class PlatformJavaObjectFactoryProxy implements PlatformJavaObjectFactory, Externalizable, Binarylizable {
    /** Class name. */
    private String clsName;

    /** Properties. */
    @GridToStringExclude
    private Map<String, Object> props;

    /** Whether object is to be created directly. */
    private boolean direct;

    /**
     * Default constructor.
     */
    public PlatformJavaObjectFactoryProxy() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Object create() {
        assert direct : "Should be called only in direct mode.";

        return create0(null);
    }
    /**
     * Get factory instance.
     *
     * @param ctx Kernal context for injections.
     * @return Factory instance.
     */
    public PlatformJavaObjectFactory factory(GridKernalContext ctx) {
        if (direct)
            return this;
        else
            return create0(ctx);
    }

    /**
     * Internal create routine.
     *
     * @param ctx Optional context for injections.
     * @return Object.
     */
    @SuppressWarnings("unchecked")
    private <T> T create0(@Nullable GridKernalContext ctx) {
        if (clsName == null)
            throw new IgniteException("Java object/factory class name is not set.");

        Class cls = U.classForName(clsName, null);

        if (cls == null)
            throw new IgniteException("Java object/factory class is not found (is it in the classpath?): " +
                clsName);

        Object res;

        try {
            res =  cls.newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteException("Failed to instantiate Java object/factory class (does it have public " +
                "default constructor?): " + clsName, e);
        }

        if (props != null) {
            for (Map.Entry<String, Object> prop : props.entrySet()) {
                String fieldName = prop.getKey();

                if (fieldName == null)
                    throw new IgniteException("Java object/factory field name cannot be null: " + clsName);

                Field field = U.findField(cls, fieldName);

                if (field == null)
                    throw new IgniteException("Java object/factory class field is not found [" +
                        "className=" + clsName + ", fieldName=" + fieldName + ']');

                try {
                    field.set(res, prop.getValue());
                }
                catch (ReflectiveOperationException e) {
                    throw new IgniteException("Failed to set Java object/factory field [className=" + clsName +
                        ", fieldName=" + fieldName + ']', e);
                }
            }
        }

        if (ctx != null) {
            try {
                ctx.resource().injectGeneric(res);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to inject resources to Java factory: " + clsName, e);
            }
        }

        return (T)res;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriterEx rawWriter = (BinaryRawWriterEx)writer.rawWriter();

        rawWriter.writeString(clsName);
        rawWriter.writeBoolean(direct);

        if (props != null) {
            rawWriter.writeInt(props.size());

            for (Map.Entry<String, Object> prop : props.entrySet()) {
                rawWriter.writeString(prop.getKey());
                rawWriter.writeObjectDetached(prop.getValue());
            }
        }
        else
            rawWriter.writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReaderEx rawReader = (BinaryRawReaderEx)reader.rawReader();

        clsName = rawReader.readString();
        direct = rawReader.readBoolean();

        int propsSize = rawReader.readInt();

        if (propsSize >= 0) {
            props = new HashMap<>(propsSize);

            for (int i = 0; i < propsSize; i++) {
                String key = rawReader.readString();
                Object val = rawReader.readObjectDetached();

                props.put(key, val);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, clsName);
        U.writeMap(out, props);
        out.writeBoolean(direct);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        clsName = U.readString(in);
        props = U.readMap(in);
        direct = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformJavaObjectFactoryProxy.class, this);
    }
}
