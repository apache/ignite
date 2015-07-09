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

package org.apache.ignite.marshaller.optimized;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.services.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * Fields indexing handler.
 */
public class OptimizedMarshallerIndexingHandler {
    /** */
    private static final ConcurrentMap<Class<?>, Boolean> indexingEnabledCache = new ConcurrentHashMap<>();

    /** Class descriptors by class. */
    private ConcurrentMap<Class, OptimizedClassDescriptor> clsMap;

    /** Metadata handler. */
    private volatile OptimizedMarshallerMetaHandler metaHandler;

    /** ID mapper. */
    private OptimizedMarshallerIdMapper mapper;

    /** Marshaller context. */
    private MarshallerContext ctx;

    /** Protocol version. */
    private OptimizedMarshallerProtocolVersion protocolVer;

    /**
     * Sets metadata handler.
     *
     * @param metaHandler Metadata handler.
     */
    public void setMetaHandler(OptimizedMarshallerMetaHandler metaHandler) {
        this.metaHandler = metaHandler;
    }

    /**
     * Returns metadata handler.
     *
     * @return Metadata handler.
     */
    public OptimizedMarshallerMetaHandler metaHandler() {
        return metaHandler;
    }

    /**
     * Sets marshaller context class map.
     * @param clsMap Class map.
     */
    public void setClassMap(ConcurrentMap<Class, OptimizedClassDescriptor> clsMap) {
        this.clsMap = clsMap;
    }

    /**
     * Sets marshaller ID mapper.
     *
     * @param mapper ID mapper.
     */
    public void setIdMapper(OptimizedMarshallerIdMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Returns ID mapper.
     *
     * @return ID mapper.
     */
    public OptimizedMarshallerIdMapper idMapper() {
        return mapper;
    }

    /**
     * Sets marshaller context.
     *
     * @param ctx Marshaller context.
     */
    public void setMarshallerCtx(MarshallerContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Sets marshaller protocol version.
     *
      * @param protocolVer Protocol version.
     */
    public void setProtocolVersion(OptimizedMarshallerProtocolVersion protocolVer) {
        this.protocolVer = protocolVer;
    }

    /**
     * Checks whether this functionality is globally supported.
     *
     * @return {@code true} if enabled.
     */
    public boolean isFieldsIndexingSupported() {
        return protocolVer != OptimizedMarshallerProtocolVersion.VER_1;
    }

    /**
     * Enables fields indexing for the object of the given {@code cls}.
     *
     * If enabled then a footer will be added during marshalling of an object of the given {@code cls} to the end of
     * its serialized form.
     *
     * @param cls Class.
     * @return {@code true} if fields indexing is enabled.
     */
    public boolean enableFieldsIndexingForClass(Class<?> cls) {
        if (!isFieldsIndexingSupported())
            return false;

        if (metaHandler == null)
            return false;

        Boolean res = indexingEnabledCache.get(cls);

        if (res != null)
            return res;

        if (isFieldsIndexingExcludedForClass(cls))
            res = false;
        else if (OptimizedMarshalAware.class.isAssignableFrom(cls))
            res = true;
        else {
            try {
                OptimizedClassDescriptor desc = OptimizedMarshallerUtils.classDescriptor(clsMap, cls, ctx, mapper,
                    this);

                if (desc.fields() != null && desc.fields().fieldsIndexingSupported()) {
                    OptimizedObjectMetadata meta = new OptimizedObjectMetadata();

                    for (OptimizedClassDescriptor.ClassFields clsFields : desc.fields().fieldsList())
                        for (OptimizedClassDescriptor.FieldInfo info : clsFields.fieldInfoList())
                            meta.addField(info.name(), info.type());

                    metaHandler.addMeta(desc.typeId(), meta);

                    res = true;
                }
                else
                    res = false;

            } catch (IOException e) {
                throw new IgniteException("Failed to put meta for class: " + cls.getName(), e);
            }
        }

        indexingEnabledCache.put(cls, res);

        return res;
    }

    /**
     * Checks whether fields indexing is excluded for class.
     *
     * @param cls Class.
     * @return {@code true} if excluded.
     */
    private boolean isFieldsIndexingExcludedForClass(Class<?> cls) {
        return ctx.isSystemType(cls.getName()) || Service.class.isAssignableFrom(cls) ||
            ComputeTask.class.isAssignableFrom(cls);
    }
}
