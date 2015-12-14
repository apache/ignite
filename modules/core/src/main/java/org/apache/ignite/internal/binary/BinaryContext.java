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

package org.apache.ignite.internal.binary;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.datastructures.CollocatedQueueItemKey;
import org.apache.ignite.internal.processors.datastructures.CollocatedSetItemKey;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Binary context.
 */
public class BinaryContext implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final ClassLoader dfltLdr = U.gridClassLoader();

    /** */
    private final ConcurrentMap<Class<?>, BinaryClassDescriptor> descByCls = new ConcurrentHashMap8<>();

    /** Holds classes loaded by default class loader only. */
    private final ConcurrentMap<Integer, BinaryClassDescriptor> userTypes = new ConcurrentHashMap8<>();

    /** */
    private final Map<Integer, BinaryClassDescriptor> predefinedTypes = new HashMap<>();

    /** */
    private final Map<String, Integer> predefinedTypeNames = new HashMap<>();

    /** */
    private final Map<Class<? extends Collection>, Byte> colTypes = new HashMap<>();

    /** */
    private final Map<Class<? extends Map>, Byte> mapTypes = new HashMap<>();

    /** */
    private final ConcurrentMap<Integer, BinaryIdMapper> mappers = new ConcurrentHashMap8<>(0);

    /** Affinity key field names. */
    private final ConcurrentMap<Integer, String> affKeyFieldNames = new ConcurrentHashMap8<>(0);

    /** */
    private final Map<String, BinaryIdMapper> typeMappers = new ConcurrentHashMap8<>(0);

    /** */
    private BinaryMetadataHandler metaHnd;

    /** Actual marshaller. */
    private BinaryMarshaller marsh;

    /** */
    private MarshallerContext marshCtx;

    /** */
    private String gridName;

    /** */
    private IgniteConfiguration igniteCfg;

    /** */
    private final OptimizedMarshaller optmMarsh = new OptimizedMarshaller();

    /** Compact footer flag. */
    private boolean compactFooter;

    /** Object schemas. */
    private volatile Map<Integer, BinarySchemaRegistry> schemas;

    /**
     * For {@link Externalizable}.
     */
    public BinaryContext() {
        // No-op.
    }

    /**
     * @param metaHnd Meta data handler.
     * @param igniteCfg Ignite configuration.
     */
    public BinaryContext(BinaryMetadataHandler metaHnd, IgniteConfiguration igniteCfg) {
        assert metaHnd != null;
        assert igniteCfg != null;

        this.metaHnd = metaHnd;
        this.igniteCfg = igniteCfg;

        gridName = igniteCfg.getGridName();

        colTypes.put(ArrayList.class, GridBinaryMarshaller.ARR_LIST);
        colTypes.put(LinkedList.class, GridBinaryMarshaller.LINKED_LIST);
        colTypes.put(HashSet.class, GridBinaryMarshaller.HASH_SET);
        colTypes.put(LinkedHashSet.class, GridBinaryMarshaller.LINKED_HASH_SET);

        mapTypes.put(HashMap.class, GridBinaryMarshaller.HASH_MAP);
        mapTypes.put(LinkedHashMap.class, GridBinaryMarshaller.LINKED_HASH_MAP);

        // IDs range from [0..200] is used by Java SDK API and GridGain legacy API

        registerPredefinedType(Byte.class, GridBinaryMarshaller.BYTE);
        registerPredefinedType(Boolean.class, GridBinaryMarshaller.BOOLEAN);
        registerPredefinedType(Short.class, GridBinaryMarshaller.SHORT);
        registerPredefinedType(Character.class, GridBinaryMarshaller.CHAR);
        registerPredefinedType(Integer.class, GridBinaryMarshaller.INT);
        registerPredefinedType(Long.class, GridBinaryMarshaller.LONG);
        registerPredefinedType(Float.class, GridBinaryMarshaller.FLOAT);
        registerPredefinedType(Double.class, GridBinaryMarshaller.DOUBLE);
        registerPredefinedType(String.class, GridBinaryMarshaller.STRING);
        registerPredefinedType(BigDecimal.class, GridBinaryMarshaller.DECIMAL);
        registerPredefinedType(Date.class, GridBinaryMarshaller.DATE);
        registerPredefinedType(Timestamp.class, GridBinaryMarshaller.TIMESTAMP);
        registerPredefinedType(UUID.class, GridBinaryMarshaller.UUID);

        registerPredefinedType(byte[].class, GridBinaryMarshaller.BYTE_ARR);
        registerPredefinedType(short[].class, GridBinaryMarshaller.SHORT_ARR);
        registerPredefinedType(int[].class, GridBinaryMarshaller.INT_ARR);
        registerPredefinedType(long[].class, GridBinaryMarshaller.LONG_ARR);
        registerPredefinedType(float[].class, GridBinaryMarshaller.FLOAT_ARR);
        registerPredefinedType(double[].class, GridBinaryMarshaller.DOUBLE_ARR);
        registerPredefinedType(char[].class, GridBinaryMarshaller.CHAR_ARR);
        registerPredefinedType(boolean[].class, GridBinaryMarshaller.BOOLEAN_ARR);
        registerPredefinedType(BigDecimal[].class, GridBinaryMarshaller.DECIMAL_ARR);
        registerPredefinedType(String[].class, GridBinaryMarshaller.STRING_ARR);
        registerPredefinedType(UUID[].class, GridBinaryMarshaller.UUID_ARR);
        registerPredefinedType(Date[].class, GridBinaryMarshaller.DATE_ARR);
        registerPredefinedType(Timestamp[].class, GridBinaryMarshaller.TIMESTAMP_ARR);
        registerPredefinedType(Object[].class, GridBinaryMarshaller.OBJ_ARR);

        registerPredefinedType(ArrayList.class, 0);
        registerPredefinedType(LinkedList.class, 0);
        registerPredefinedType(HashSet.class, 0);
        registerPredefinedType(LinkedHashSet.class, 0);

        registerPredefinedType(HashMap.class, 0);
        registerPredefinedType(LinkedHashMap.class, 0);

        registerPredefinedType(GridMapEntry.class, 60);
        registerPredefinedType(IgniteBiTuple.class, 61);
        registerPredefinedType(T2.class, 62);

        // IDs range [200..1000] is used by Ignite internal APIs.
    }

    /**
     * @return Marshaller.
     */
    public BinaryMarshaller marshaller() {
        return marsh;
    }

    /**
     * @return Ignite configuration.
     */
    public IgniteConfiguration configuration(){
        return igniteCfg;
    }

    /**
     * @param marsh Binary marshaller.
     * @param cfg Configuration.
     * @throws BinaryObjectException In case of error.
     */
    public void configure(BinaryMarshaller marsh, IgniteConfiguration cfg) throws BinaryObjectException {
        if (marsh == null)
            return;

        this.marsh = marsh;

        marshCtx = marsh.getContext();

        BinaryConfiguration binaryCfg = cfg.getBinaryConfiguration();

        if (binaryCfg == null)
            binaryCfg = new BinaryConfiguration();

        assert marshCtx != null;

        optmMarsh.setContext(marshCtx);

        configure(
            binaryCfg.getIdMapper(),
            binaryCfg.getSerializer(),
            binaryCfg.getTypeConfigurations()
        );

        compactFooter = binaryCfg.isCompactFooter();
    }

    /**
     * @param globalIdMapper ID mapper.
     * @param globalSerializer Serializer.
     * @param typeCfgs Type configurations.
     * @throws BinaryObjectException In case of error.
     */
    private void configure(
        BinaryIdMapper globalIdMapper,
        BinarySerializer globalSerializer,
        Collection<BinaryTypeConfiguration> typeCfgs
    ) throws BinaryObjectException {
        TypeDescriptors descs = new TypeDescriptors();

        Map<String, String> affFields = new HashMap<>();

        if (!F.isEmpty(igniteCfg.getCacheKeyConfiguration())) {
            for (CacheKeyConfiguration keyCfg : igniteCfg.getCacheKeyConfiguration())
                affFields.put(keyCfg.getTypeName(), keyCfg.getAffinityKeyFieldName());
        }

        if (typeCfgs != null) {
            for (BinaryTypeConfiguration typeCfg : typeCfgs) {
                String clsName = typeCfg.getTypeName();

                if (clsName == null)
                    throw new BinaryObjectException("Class name is required for binary type configuration.");

                BinaryIdMapper idMapper = globalIdMapper;

                if (typeCfg.getIdMapper() != null)
                    idMapper = typeCfg.getIdMapper();

                idMapper = BinaryInternalIdMapper.create(idMapper);

                BinarySerializer serializer = globalSerializer;

                if (typeCfg.getSerializer() != null)
                    serializer = typeCfg.getSerializer();

                if (clsName.endsWith(".*")) {
                    String pkgName = clsName.substring(0, clsName.length() - 2);

                    for (String clsName0 : classesInPackage(pkgName))
                        descs.add(clsName0, idMapper, serializer, affFields.get(clsName0),
                            typeCfg.isEnum(), true);
                }
                else
                    descs.add(clsName, idMapper, serializer, affFields.get(clsName),
                        typeCfg.isEnum(), false);
            }
        }

        for (TypeDescriptor desc : descs.descriptors())
            registerUserType(desc.clsName, desc.idMapper, desc.serializer, desc.affKeyFieldName, desc.isEnum);

        BinaryInternalIdMapper dfltMapper = BinaryInternalIdMapper.create(globalIdMapper);

        // Put affinity field names for unconfigured types.
        for (Map.Entry<String, String> entry : affFields.entrySet()) {
            String typeName = entry.getKey();

            int typeId = dfltMapper.typeId(typeName);

            affKeyFieldNames.putIfAbsent(typeId, entry.getValue());
        }

        addSystemClassAffinityKey(CollocatedSetItemKey.class);
        addSystemClassAffinityKey(CollocatedQueueItemKey.class);
    }

    /**
     * @param cls Class.
     */
    private void addSystemClassAffinityKey(Class<?> cls) {
        String fieldName = affinityFieldName(cls);

        assert fieldName != null : cls;

        affKeyFieldNames.putIfAbsent(cls.getName().hashCode(), affinityFieldName(cls));
    }

    /**
     * @param pkgName Package name.
     * @return Class names.
     */
    @SuppressWarnings("ConstantConditions")
    private static Iterable<String> classesInPackage(String pkgName) {
        assert pkgName != null;

        Collection<String> clsNames = new ArrayList<>();

        ClassLoader ldr = U.gridClassLoader();

        if (ldr instanceof URLClassLoader) {
            String pkgPath = pkgName.replaceAll("\\.", "/");

            URL[] urls = ((URLClassLoader)ldr).getURLs();

            for (URL url : urls) {
                String proto = url.getProtocol().toLowerCase();

                if ("file".equals(proto)) {
                    try {
                        File cpElement = new File(url.toURI());

                        if (cpElement.isDirectory()) {
                            File pkgDir = new File(cpElement, pkgPath);

                            if (pkgDir.isDirectory()) {
                                for (File file : pkgDir.listFiles()) {
                                    String fileName = file.getName();

                                    if (file.isFile() && fileName.toLowerCase().endsWith(".class"))
                                        clsNames.add(pkgName + '.' + fileName.substring(0, fileName.length() - 6));
                                }
                            }
                        }
                        else if (cpElement.isFile()) {
                            try {
                                JarFile jar = new JarFile(cpElement);

                                Enumeration<JarEntry> entries = jar.entries();

                                while (entries.hasMoreElements()) {
                                    String entry = entries.nextElement().getName();

                                    if (entry.startsWith(pkgPath) && entry.endsWith(".class")) {
                                        String clsName = entry.substring(pkgPath.length() + 1, entry.length() - 6);

                                        if (!clsName.contains("/") && !clsName.contains("\\"))
                                            clsNames.add(pkgName + '.' + clsName);
                                    }
                                }
                            }
                            catch (IOException ignored) {
                                // No-op.
                            }
                        }
                    }
                    catch (URISyntaxException ignored) {
                        // No-op.
                    }
                }
            }
        }

        return clsNames;
    }

    /**
     * @param cls Class.
     * @return Class descriptor.
     * @throws BinaryObjectException In case of error.
     */
    public BinaryClassDescriptor descriptorForClass(Class<?> cls, boolean deserialize)
        throws BinaryObjectException {
        assert cls != null;

        BinaryClassDescriptor desc = descByCls.get(cls);

        if (desc == null || !desc.registered())
            desc = registerClassDescriptor(cls, deserialize);

        return desc;
    }

    /**
     * @param userType User type or not.
     * @param typeId Type ID.
     * @param ldr Class loader.
     * @return Class descriptor.
     */
    public BinaryClassDescriptor descriptorForTypeId(
        boolean userType,
        int typeId,
        ClassLoader ldr,
        boolean deserialize
    ) {
        assert typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID;

        //TODO: As a workaround for IGNITE-1358 we always check the predefined map before without checking 'userType'
        BinaryClassDescriptor desc = predefinedTypes.get(typeId);

        if (desc != null)
            return desc;

        if (ldr == null)
            ldr = dfltLdr;

        // If the type hasn't been loaded by default class loader then we mustn't return the descriptor from here
        // giving a chance to a custom class loader to reload type's class.
        if (userType && ldr.equals(dfltLdr)) {
            desc = userTypes.get(typeId);

            if (desc != null)
                return desc;
        }

        Class cls;

        try {
            cls = marshCtx.getClass(typeId, ldr);

            desc = descByCls.get(cls);
        }
        catch (ClassNotFoundException e) {
            // Class might have been loaded by default class loader.
            if (userType && !ldr.equals(dfltLdr) && (desc = descriptorForTypeId(true, typeId, dfltLdr, deserialize)) != null)
                return desc;

            throw new BinaryInvalidTypeException(e);
        }
        catch (IgniteCheckedException e) {
            // Class might have been loaded by default class loader.
            if (userType && !ldr.equals(dfltLdr) && (desc = descriptorForTypeId(true, typeId, dfltLdr, deserialize)) != null)
                return desc;

            throw new BinaryObjectException("Failed resolve class for ID: " + typeId, e);
        }

        if (desc == null) {
            desc = registerClassDescriptor(cls, deserialize);

            assert desc.typeId() == typeId;
        }

        return desc;
    }

    /**
     * Creates and registers {@link BinaryClassDescriptor} for the given {@code class}.
     *
     * @param cls Class.
     * @return Class descriptor.
     */
    private BinaryClassDescriptor registerClassDescriptor(Class<?> cls, boolean deserialize) {
        BinaryClassDescriptor desc;

        String clsName = cls.getName();

        if (marshCtx.isSystemType(clsName)) {
            desc = new BinaryClassDescriptor(this,
                cls,
                false,
                clsName.hashCode(),
                clsName,
                null,
                BinaryInternalIdMapper.defaultInstance(),
                null,
                false,
                true, /* registered */
                false /* predefined */
            );

            BinaryClassDescriptor old = descByCls.putIfAbsent(cls, desc);

            if (old != null)
                desc = old;
        }
        else
            desc = registerUserClassDescriptor(cls, deserialize);

        return desc;
    }

    /**
     * Creates and registers {@link BinaryClassDescriptor} for the given user {@code class}.
     *
     * @param cls Class.
     * @return Class descriptor.
     */
    private BinaryClassDescriptor registerUserClassDescriptor(Class<?> cls, boolean deserialize) {
        boolean registered;

        String typeName = typeName(cls.getName());

        BinaryIdMapper idMapper = userTypeIdMapper(typeName);

        int typeId = idMapper.typeId(typeName);

        try {
            registered = marshCtx.registerClass(typeId, cls);
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException("Failed to register class.", e);
        }

        String affFieldName = affinityFieldName(cls);

        BinaryClassDescriptor desc = new BinaryClassDescriptor(this,
            cls,
            true,
            typeId,
            typeName,
            affFieldName,
            idMapper,
            null,
            true,
            registered,
            false /* predefined */
        );

        if (!deserialize) {
            Collection<BinarySchema> schemas = desc.schema() != null ? Collections.singleton(desc.schema()) : null;

            metaHnd.addMeta(typeId,
                new BinaryMetadata(typeId, typeName, desc.fieldsMeta(), affFieldName, schemas, desc.isEnum()).wrap(this));
        }

        // perform put() instead of putIfAbsent() because "registered" flag might have been changed or class loader
        // might have reloaded described class.
        if (IgniteUtils.detectClassLoader(cls).equals(dfltLdr))
            userTypes.put(typeId, desc);

        descByCls.put(cls, desc);

        mappers.putIfAbsent(typeId, idMapper);

        return desc;
    }

    /**
     * @param cls Collection class.
     * @return Collection type ID.
     */
    public byte collectionType(Class<? extends Collection> cls) {
        assert cls != null;

        Byte type = colTypes.get(cls);

        if (type != null)
            return type;

        return Set.class.isAssignableFrom(cls) ? GridBinaryMarshaller.USER_SET : GridBinaryMarshaller.USER_COL;
    }

    /**
     * @param cls Map class.
     * @return Map type ID.
     */
    public byte mapType(Class<? extends Map> cls) {
        assert cls != null;

        Byte type = mapTypes.get(cls);

        return type != null ? type : GridBinaryMarshaller.USER_COL;
    }

    /**
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName) {
        String typeName0 = typeName(typeName);

        Integer id = predefinedTypeNames.get(typeName0);

        if (id != null)
            return id;

        if (marshCtx.isSystemType(typeName))
            return typeName.hashCode();

        return userTypeIdMapper(typeName0).typeId(typeName0);
    }

    /**
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    public int fieldId(int typeId, String fieldName) {
        return userTypeIdMapper(typeId).fieldId(typeId, fieldName);
    }

    /**
     * @param typeId Type ID.
     * @return Instance of ID mapper.
     */
    public BinaryIdMapper userTypeIdMapper(int typeId) {
        BinaryIdMapper idMapper = mappers.get(typeId);

        return idMapper != null ? idMapper : BinaryInternalIdMapper.defaultInstance();
    }

    /**
     * @param typeName Type name.
     * @return Instance of ID mapper.
     */
    private BinaryIdMapper userTypeIdMapper(String typeName) {
        BinaryIdMapper idMapper = typeMappers.get(typeName);

        return idMapper != null ? idMapper : BinaryInternalIdMapper.defaultInstance();
    }

    /**
     * @param cls Class to get affinity field for.
     * @return Affinity field name or {@code null} if field name was not found.
     */
    private String affinityFieldName(Class cls) {
        for (; cls != Object.class && cls != null; cls = cls.getSuperclass()) {
            for (Field f : cls.getDeclaredFields()) {
                if (f.getAnnotation(AffinityKeyMapped.class) != null)
                    return f.getName();
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, igniteCfg.getGridName());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        gridName = U.readString(in);
    }

    /**
     * @return Binary context.
     * @throws ObjectStreamException In case of error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            IgniteKernal g = IgnitionEx.gridx(gridName);

            if (g == null)
                throw new IllegalStateException("Failed to find grid for name: " + gridName);

            return ((CacheObjectBinaryProcessorImpl)g.context().cacheObjects()).binaryContext();
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
    }

    /**
     * @param cls Class.
     * @param id Type ID.
     * @return GridBinaryClassDescriptor.
     */
    public BinaryClassDescriptor registerPredefinedType(Class<?> cls, int id) {
        String typeName = typeName(cls.getName());

        BinaryClassDescriptor desc = new BinaryClassDescriptor(
            this,
            cls,
            false,
            id,
            typeName,
            null,
            BinaryInternalIdMapper.defaultInstance(),
            null,
            false,
            true, /* registered */
            true /* predefined */
        );

        predefinedTypeNames.put(typeName, id);
        predefinedTypes.put(id, desc);

        descByCls.put(cls, desc);

        return desc;
    }

    /**
     * @param clsName Class name.
     * @param idMapper ID mapper.
     * @param serializer Serializer.
     * @param affKeyFieldName Affinity key field name.
     * @param isEnum If enum.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("ErrorNotRethrown")
    public void registerUserType(String clsName,
        BinaryIdMapper idMapper,
        @Nullable BinarySerializer serializer,
        @Nullable String affKeyFieldName,
        boolean isEnum)
        throws BinaryObjectException {
        assert idMapper != null;

        Class<?> cls = null;

        try {
            cls = Class.forName(clsName);
        }
        catch (ClassNotFoundException | NoClassDefFoundError ignored) {
            // No-op.
        }

        String typeName = typeName(clsName);

        int id = idMapper.typeId(typeName);

        //Workaround for IGNITE-1358
        if (predefinedTypes.get(id) != null)
            throw new BinaryObjectException("Duplicate type ID [clsName=" + clsName + ", id=" + id + ']');

        if (mappers.put(id, idMapper) != null)
            throw new BinaryObjectException("Duplicate type ID [clsName=" + clsName + ", id=" + id + ']');

        if (affKeyFieldName != null) {
            if (affKeyFieldNames.put(id, affKeyFieldName) != null)
                throw new BinaryObjectException("Duplicate type ID [clsName=" + clsName + ", id=" + id + ']');
        }

        typeMappers.put(typeName, idMapper);

        Map<String, Integer> fieldsMeta = null;
        Collection<BinarySchema> schemas = null;

        if (cls != null) {
            BinaryClassDescriptor desc = new BinaryClassDescriptor(
                this,
                cls,
                true,
                id,
                typeName,
                affKeyFieldName,
                idMapper,
                serializer,
                true,
                true, /* registered */
                false /* predefined */
            );

            fieldsMeta = desc.fieldsMeta();
            schemas = desc.schema() != null ? Collections.singleton(desc.schema()) : null;

            if (IgniteUtils.detectClassLoader(cls).equals(dfltLdr))
                userTypes.put(id, desc);

            descByCls.put(cls, desc);
        }

        metaHnd.addMeta(id, new BinaryMetadata(id, typeName, fieldsMeta, affKeyFieldName, schemas, isEnum).wrap(this));
    }

    /**
     * Create binary field.
     *
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Binary field.
     */
    public BinaryFieldImpl createField(int typeId, String fieldName) {
        BinarySchemaRegistry schemaReg = schemaRegistry(typeId);

        int fieldId = userTypeIdMapper(typeId).fieldId(typeId, fieldName);

        return new BinaryFieldImpl(typeId, schemaReg, fieldName, fieldId);
    }

    /**
     * @param typeId Type ID.
     * @return Meta data.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public BinaryType metadata(int typeId) throws BinaryObjectException {
        return metaHnd != null ? metaHnd.metadata(typeId) : null;
    }

    /**
     * @param typeId Type ID.
     * @return Affinity key field name.
     */
    public String affinityKeyFieldName(int typeId) {
        return affKeyFieldNames.get(typeId);
    }

    /**
     * @param typeId Type ID.
     * @param meta Meta data.
     * @throws BinaryObjectException In case of error.
     */
    public void updateMetadata(int typeId, BinaryMetadata meta) throws BinaryObjectException {
        metaHnd.addMeta(typeId, meta.wrap(this));
    }

    /**
     * @return Whether field IDs should be skipped in footer or not.
     */
    public boolean isCompactFooter() {
        return compactFooter;
    }

    /**
     * Get schema registry for type ID.
     *
     * @param typeId Type ID.
     * @return Schema registry for type ID.
     */
    public BinarySchemaRegistry schemaRegistry(int typeId) {
        Map<Integer, BinarySchemaRegistry> schemas0 = schemas;

        if (schemas0 == null) {
            synchronized (this) {
                schemas0 = schemas;

                if (schemas0 == null) {
                    schemas0 = new HashMap<>();

                    BinarySchemaRegistry reg = new BinarySchemaRegistry();

                    schemas0.put(typeId, reg);

                    schemas = schemas0;

                    return reg;
                }
            }
        }

        BinarySchemaRegistry reg = schemas0.get(typeId);

        if (reg == null) {
            synchronized (this) {
                reg = schemas.get(typeId);

                if (reg == null) {
                    reg = new BinarySchemaRegistry();

                    schemas0 = new HashMap<>(schemas);

                    schemas0.put(typeId, reg);

                    schemas = schemas0;
                }
            }
        }

        return reg;
    }

    /**
     * Returns instance of {@link OptimizedMarshaller}.
     *
     * @return Optimized marshaller.
     */
    OptimizedMarshaller optimizedMarsh() {
        return optmMarsh;
    }

    /**
     * @param clsName Class name.
     * @return Type name.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static String typeName(String clsName) {
        assert clsName != null;

        int idx = clsName.lastIndexOf('$');

        if (idx == clsName.length() - 1)
            // This is a regular (not inner) class name that ends with '$'. Common use case for Scala classes.
            idx = -1;
        else if (idx >= 0) {
            String typeName = clsName.substring(idx + 1);

            try {
                Integer.parseInt(typeName);

                // This is an anonymous class. Don't cut off enclosing class name for it.
                idx = -1;
            }
            catch (NumberFormatException ignore) {
                // This is a lambda class.
                if (clsName.indexOf("$$Lambda$") > 0)
                    idx = -1;
                else
                    return typeName;
            }
        }

        if (idx < 0)
            idx = clsName.lastIndexOf('.');

        return idx >= 0 ? clsName.substring(idx + 1) : clsName;
    }

    /**
     * Undeployment callback invoked when class loader is being undeployed.
     *
     * Some marshallers may want to clean their internal state that uses the undeployed class loader somehow.
     *
     * @param ldr Class loader being undeployed.
     */
    public void onUndeploy(ClassLoader ldr) {
        for (Class<?> cls : descByCls.keySet()) {
            if (ldr.equals(cls.getClassLoader()))
                descByCls.remove(cls);
        }

        U.clearClassCache(ldr);
    }

    /**
     * Type descriptors.
     */
    private static class TypeDescriptors {
        /** Descriptors map. */
        private final Map<String, TypeDescriptor> descs = new LinkedHashMap<>();

        /**
         * Add type descriptor.
         *
         * @param clsName Class name.
         * @param idMapper ID mapper.
         * @param serializer Serializer.
         * @param affKeyFieldName Affinity key field name.
         * @param isEnum Enum flag.
         * @param canOverride Whether this descriptor can be override.
         * @throws BinaryObjectException If failed.
         */
        private void add(String clsName,
            BinaryIdMapper idMapper,
            BinarySerializer serializer,
            String affKeyFieldName,
            boolean isEnum,
            boolean canOverride)
            throws BinaryObjectException {
            TypeDescriptor desc = new TypeDescriptor(clsName,
                idMapper,
                serializer,
                affKeyFieldName,
                isEnum,
                canOverride);

            TypeDescriptor oldDesc = descs.get(clsName);

            if (oldDesc == null)
                descs.put(clsName, desc);
            else
                oldDesc.override(desc);
        }

        /**
         * Get all collected descriptors.
         *
         * @return Descriptors.
         */
        private Iterable<TypeDescriptor> descriptors() {
            return descs.values();
        }
    }

    /**
     * Type descriptor.
     */
    private static class TypeDescriptor {
        /** Class name. */
        private final String clsName;

        /** ID mapper. */
        private BinaryIdMapper idMapper;

        /** Serializer. */
        private BinarySerializer serializer;

        /** Affinity key field name. */
        private String affKeyFieldName;

        /** Enum flag. */
        private boolean isEnum;

        /** Whether this descriptor can be override. */
        private boolean canOverride;

        /**
         * Constructor.
         *
         * @param clsName Class name.
         * @param idMapper ID mapper.
         * @param serializer Serializer.
         * @param affKeyFieldName Affinity key field name.
         * @param isEnum Enum type.
         * @param canOverride Whether this descriptor can be override.
         */
        private TypeDescriptor(String clsName, BinaryIdMapper idMapper, BinarySerializer serializer,
            String affKeyFieldName, boolean isEnum, boolean canOverride) {
            this.clsName = clsName;
            this.idMapper = idMapper;
            this.serializer = serializer;
            this.affKeyFieldName = affKeyFieldName;
            this.isEnum = isEnum;
            this.canOverride = canOverride;
        }

        /**
         * Override binary class descriptor.
         *
         * @param other Other descriptor.
         * @throws BinaryObjectException If failed.
         */
        private void override(TypeDescriptor other) throws BinaryObjectException {
            assert clsName.equals(other.clsName);

            if (canOverride) {
                idMapper = other.idMapper;
                serializer = other.serializer;
                affKeyFieldName = other.affKeyFieldName;
                canOverride = other.canOverride;
            }
            else if (!other.canOverride)
                throw new BinaryObjectException("Duplicate explicit class definition in configuration: " + clsName);
        }
    }

    /**
     * Type id wrapper.
     */
    static class Type {
        /** Type id */
        private final int id;

        /** Whether the following type is registered in a cache or not */
        private final boolean registered;

        /**
         * @param id Id.
         * @param registered Registered.
         */
        public Type(int id, boolean registered) {
            this.id = id;
            this.registered = registered;
        }

        /**
         * @return Type ID.
         */
        public int id() {
            return id;
        }

        /**
         * @return Registered flag value.
         */
        public boolean registered() {
            return registered;
        }
    }
}
