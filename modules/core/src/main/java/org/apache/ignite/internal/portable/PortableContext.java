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

package org.apache.ignite.internal.portable;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.portable.CacheObjectPortableProcessorImpl;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.platform.dotnet.PlatformDotNetConfiguration;
import org.apache.ignite.platform.dotnet.PlatformDotNetPortableConfiguration;
import org.apache.ignite.platform.dotnet.PlatformDotNetPortableTypeConfiguration;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableIdMapper;
import org.apache.ignite.portable.PortableInvalidClassException;
import org.apache.ignite.portable.PortableMetadata;
import org.apache.ignite.portable.PortableSerializer;
import org.apache.ignite.portable.PortableTypeConfiguration;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * Portable context.
 */
public class PortableContext implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    static final PortableIdMapper DFLT_ID_MAPPER = new IdMapperWrapper(null);

    /** */
    static final PortableIdMapper BASIC_CLS_ID_MAPPER = new BasicClassIdMapper();

    /** */
    static final char[] LOWER_CASE_CHARS;

    /** */
    static final char MAX_LOWER_CASE_CHAR = 0x7e;

    /**
     *
     */
    static {
        LOWER_CASE_CHARS = new char[MAX_LOWER_CASE_CHAR + 1];

        for (char c = 0; c <= MAX_LOWER_CASE_CHAR; c++)
            LOWER_CASE_CHARS[c] = Character.toLowerCase(c);
    }

    /** */
    private final ConcurrentMap<Integer, Collection<Integer>> metaDataCache = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<Class<?>, PortableClassDescriptor> descByCls = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<Integer, PortableClassDescriptor> userTypes = new ConcurrentHashMap8<>(0);

    /** */
    private final Map<Integer, PortableClassDescriptor> predefinedTypes = new HashMap<>();

    /** */
    private final Map<String, Integer> predefinedTypeNames = new HashMap<>();

    /** */
    private final Map<Class<? extends Collection>, Byte> colTypes = new HashMap<>();

    /** */
    private final Map<Class<? extends Map>, Byte> mapTypes = new HashMap<>();

    /** */
    private final Map<Integer, PortableIdMapper> mappers = new ConcurrentHashMap8<>(0);

    /** */
    private final Map<String, PortableIdMapper> typeMappers = new ConcurrentHashMap8<>(0);

    /** */
    private Map<Integer, Boolean> metaEnabled = new HashMap<>(0);

    /** */
    private Set<Integer> usingTs = new HashSet<>();

    /** */
    private PortableMetaDataHandler metaHnd;

    /** */
    private MarshallerContext marshCtx;

    /** */
    private String gridName;

    /** */
    private final OptimizedMarshaller optmMarsh = new OptimizedMarshaller();

    /** */
    private boolean convertStrings;

    /** */
    private boolean useTs;

    /** */
    private boolean metaDataEnabled;

    /** */
    private boolean keepDeserialized;

    /**
     * For {@link Externalizable}.
     */
    public PortableContext() {
        // No-op.
    }

    /**
     * @param metaHnd Meta data handler.
     * @param gridName Grid name.
     */
    public PortableContext(PortableMetaDataHandler metaHnd, @Nullable String gridName) {
        assert metaHnd != null;

        this.metaHnd = metaHnd;
        this.gridName = gridName;

        colTypes.put(ArrayList.class, GridPortableMarshaller.ARR_LIST);
        colTypes.put(LinkedList.class, GridPortableMarshaller.LINKED_LIST);
        colTypes.put(HashSet.class, GridPortableMarshaller.HASH_SET);
        colTypes.put(LinkedHashSet.class, GridPortableMarshaller.LINKED_HASH_SET);
        colTypes.put(TreeSet.class, GridPortableMarshaller.TREE_SET);
        colTypes.put(ConcurrentSkipListSet.class, GridPortableMarshaller.CONC_SKIP_LIST_SET);

        mapTypes.put(HashMap.class, GridPortableMarshaller.HASH_MAP);
        mapTypes.put(LinkedHashMap.class, GridPortableMarshaller.LINKED_HASH_MAP);
        mapTypes.put(TreeMap.class, GridPortableMarshaller.TREE_MAP);
        mapTypes.put(ConcurrentHashMap.class, GridPortableMarshaller.CONC_HASH_MAP);
        mapTypes.put(ConcurrentHashMap8.class, GridPortableMarshaller.CONC_HASH_MAP);
        mapTypes.put(Properties.class, GridPortableMarshaller.PROPERTIES_MAP);

        // IDs range from [0..200] is used by Java SDK API and GridGain legacy API

        registerPredefinedType(Byte.class, GridPortableMarshaller.BYTE);
        registerPredefinedType(Boolean.class, GridPortableMarshaller.BOOLEAN);
        registerPredefinedType(Short.class, GridPortableMarshaller.SHORT);
        registerPredefinedType(Character.class, GridPortableMarshaller.CHAR);
        registerPredefinedType(Integer.class, GridPortableMarshaller.INT);
        registerPredefinedType(Long.class, GridPortableMarshaller.LONG);
        registerPredefinedType(Float.class, GridPortableMarshaller.FLOAT);
        registerPredefinedType(Double.class, GridPortableMarshaller.DOUBLE);
        registerPredefinedType(String.class, GridPortableMarshaller.STRING);
        registerPredefinedType(BigDecimal.class, GridPortableMarshaller.DECIMAL);
        registerPredefinedType(Date.class, GridPortableMarshaller.DATE);
        registerPredefinedType(UUID.class, GridPortableMarshaller.UUID);
        // TODO: How to handle timestamp? It has the same ID in .Net.
        registerPredefinedType(Timestamp.class, GridPortableMarshaller.DATE);

        registerPredefinedType(byte[].class, GridPortableMarshaller.BYTE_ARR);
        registerPredefinedType(short[].class, GridPortableMarshaller.SHORT_ARR);
        registerPredefinedType(int[].class, GridPortableMarshaller.INT_ARR);
        registerPredefinedType(long[].class, GridPortableMarshaller.LONG_ARR);
        registerPredefinedType(float[].class, GridPortableMarshaller.FLOAT_ARR);
        registerPredefinedType(double[].class, GridPortableMarshaller.DOUBLE_ARR);
        registerPredefinedType(char[].class, GridPortableMarshaller.CHAR_ARR);
        registerPredefinedType(boolean[].class, GridPortableMarshaller.BOOLEAN_ARR);
        registerPredefinedType(BigDecimal[].class, GridPortableMarshaller.DECIMAL_ARR);
        registerPredefinedType(String[].class, GridPortableMarshaller.STRING_ARR);
        registerPredefinedType(UUID[].class, GridPortableMarshaller.UUID_ARR);
        registerPredefinedType(Date[].class, GridPortableMarshaller.DATE_ARR);
        registerPredefinedType(Object[].class, GridPortableMarshaller.OBJ_ARR);

        registerPredefinedType(ArrayList.class, 0);
        registerPredefinedType(LinkedList.class, 0);
        registerPredefinedType(HashSet.class, 0);
        registerPredefinedType(LinkedHashSet.class, 0);
        registerPredefinedType(TreeSet.class, 0);
        registerPredefinedType(ConcurrentSkipListSet.class, 0);

        registerPredefinedType(HashMap.class, 0);
        registerPredefinedType(LinkedHashMap.class, 0);
        registerPredefinedType(TreeMap.class, 0);
        registerPredefinedType(ConcurrentHashMap.class, 0);
        registerPredefinedType(ConcurrentHashMap8.class, 0);

        registerPredefinedType(GridMapEntry.class, 60);
        registerPredefinedType(IgniteBiTuple.class, 61);
        registerPredefinedType(T2.class, 62);

        // IDs range [200..1000] is used by Ignite internal APIs.

        registerPredefinedType(PortableObjectImpl.class, 200);
        registerPredefinedType(PortableMetaDataImpl.class, 201);

        registerPredefinedType(PlatformDotNetConfiguration.class, 202);
        registerPredefinedType(PlatformDotNetPortableConfiguration.class, 203);
        registerPredefinedType(PlatformDotNetPortableTypeConfiguration.class, 204);
    }

    /**
     * @param marsh Portable marshaller.
     * @throws PortableException In case of error.
     */
    public void configure(PortableMarshaller marsh) throws PortableException {
        if (marsh == null)
            return;

        convertStrings = marsh.isConvertStringToBytes();
        useTs = marsh.isUseTimestamp();
        metaDataEnabled = marsh.isMetaDataEnabled();
        keepDeserialized = marsh.isKeepDeserialized();

        marshCtx = marsh.getContext();

        assert marshCtx != null;

        optmMarsh.setContext(marshCtx);

        configure(
            marsh.getIdMapper(),
            marsh.getSerializer(),
            marsh.isUseTimestamp(),
            marsh.isMetaDataEnabled(),
            marsh.isKeepDeserialized(),
            marsh.getClassNames(),
            marsh.getTypeConfigurations()
        );
    }

    /**
     * @param globalIdMapper ID mapper.
     * @param globalSerializer Serializer.
     * @param globalUseTs Use timestamp flag.
     * @param globalMetaDataEnabled Metadata enabled flag.
     * @param globalKeepDeserialized Keep deserialized flag.
     * @param clsNames Class names.
     * @param typeCfgs Type configurations.
     * @throws PortableException In case of error.
     */
    private void configure(
        PortableIdMapper globalIdMapper,
        PortableSerializer globalSerializer,
        boolean globalUseTs,
        boolean globalMetaDataEnabled,
        boolean globalKeepDeserialized,
        Collection<String> clsNames,
        Collection<PortableTypeConfiguration> typeCfgs
    ) throws PortableException {
        TypeDescriptors descs = new TypeDescriptors();

        if (clsNames != null) {
            PortableIdMapper idMapper = new IdMapperWrapper(globalIdMapper);

            for (String clsName : clsNames) {
                if (clsName.endsWith(".*")) { // Package wildcard
                    String pkgName = clsName.substring(0, clsName.length() - 2);

                    for (String clsName0 : classesInPackage(pkgName))
                        descs.add(clsName0, idMapper, null, null, globalUseTs, globalMetaDataEnabled,
                            globalKeepDeserialized, true);
                }
                else // Regular single class
                    descs.add(clsName, idMapper, null, null, globalUseTs, globalMetaDataEnabled,
                        globalKeepDeserialized, true);
            }
        }

        if (typeCfgs != null) {
            for (PortableTypeConfiguration typeCfg : typeCfgs) {
                String clsName = typeCfg.getClassName();

                if (clsName == null)
                    throw new PortableException("Class name is required for portable type configuration.");

                PortableIdMapper idMapper = globalIdMapper;

                if (typeCfg.getIdMapper() != null)
                    idMapper = typeCfg.getIdMapper();

                idMapper = new IdMapperWrapper(idMapper);

                PortableSerializer serializer = globalSerializer;

                if (typeCfg.getSerializer() != null)
                    serializer = typeCfg.getSerializer();

                boolean useTs = typeCfg.isUseTimestamp() != null ? typeCfg.isUseTimestamp() : globalUseTs;
                boolean metaDataEnabled = typeCfg.isMetaDataEnabled() != null ? typeCfg.isMetaDataEnabled() :
                    globalMetaDataEnabled;
                boolean keepDeserialized = typeCfg.isKeepDeserialized() != null ? typeCfg.isKeepDeserialized() :
                    globalKeepDeserialized;

                if (clsName.endsWith(".*")) {
                    String pkgName = clsName.substring(0, clsName.length() - 2);

                    for (String clsName0 : classesInPackage(pkgName))
                        descs.add(clsName0, idMapper, serializer, typeCfg.getAffinityKeyFieldName(), useTs,
                            metaDataEnabled, keepDeserialized, true);
                }
                else
                    descs.add(clsName, idMapper, serializer, typeCfg.getAffinityKeyFieldName(), useTs,
                        metaDataEnabled, keepDeserialized, false);
            }
        }

        for (TypeDescriptor desc : descs.descriptors()) {
            registerUserType(desc.clsName, desc.idMapper, desc.serializer, desc.affKeyFieldName, desc.useTs,
                desc.metadataEnabled, desc.keepDeserialized);
        }
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
     * @throws PortableException In case of error.
     */
    public PortableClassDescriptor descriptorForClass(Class<?> cls)
        throws PortableException {
        assert cls != null;

        PortableClassDescriptor desc = descByCls.get(cls);

        if (desc == null || !desc.registered())
            desc = registerClassDescriptor(cls);

        return desc;
    }

    /**
     * @param userType User type or not.
     * @param typeId Type ID.
     * @param ldr Class loader.
     * @return Class descriptor.
     */
    public PortableClassDescriptor descriptorForTypeId(boolean userType, int typeId, ClassLoader ldr) {
        assert typeId != GridPortableMarshaller.UNREGISTERED_TYPE_ID;

        //TODO: IGNITE-1358 (uncomment when fixed)
        //PortableClassDescriptor desc = userType ? userTypes.get(typeId) : predefinedTypes.get(typeId);

        // As a workaround for IGNITE-1358 we always check the predefined map before.
        PortableClassDescriptor desc = predefinedTypes.get(typeId);

        if (desc != null)
            return desc;

        if (userType) {
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
            throw new PortableInvalidClassException(e);
        }
        catch (IgniteCheckedException e) {
            throw new PortableException("Failed resolve class for ID: " + typeId, e);
        }

        if (desc == null) {
            desc = registerClassDescriptor(cls);

            assert desc.typeId() == typeId;
        }

        return desc;
    }

    /**
     * Creates and registers {@link PortableClassDescriptor} for the given {@code class}.
     *
     * @param cls Class.
     * @return Class descriptor.
     */
    private PortableClassDescriptor registerClassDescriptor(Class<?> cls) {
        PortableClassDescriptor desc;

        String clsName = cls.getName();

        if (marshCtx.isSystemType(clsName)) {
            desc = new PortableClassDescriptor(this,
                cls,
                false,
                clsName.hashCode(),
                clsName,
                BASIC_CLS_ID_MAPPER,
                null,
                useTs,
                metaDataEnabled,
                keepDeserialized,
                true, /* registered */
                false /* predefined */
            );

            PortableClassDescriptor old = descByCls.putIfAbsent(cls, desc);

            if (old != null)
                desc = old;
        }
        else
            desc = registerUserClassDescriptor(cls);

        return desc;
    }

    /**
     * Creates and registers {@link PortableClassDescriptor} for the given user {@code class}.
     *
     * @param cls Class.
     * @return Class descriptor.
     */
    private PortableClassDescriptor registerUserClassDescriptor(Class<?> cls) {
        PortableClassDescriptor desc;

        boolean registered;

        String typeName = typeName(cls.getName());

        PortableIdMapper idMapper = idMapper(typeName);

        int typeId = idMapper.typeId(typeName);

        try {
            registered = marshCtx.registerClass(typeId, cls);

        }
        catch (IgniteCheckedException e) {
            throw new PortableException("Failed to register class.", e);
        }

        desc = new PortableClassDescriptor(this,
            cls,
            true,
            typeId,
            typeName,
            idMapper,
            null,
            useTs,
            metaDataEnabled,
            keepDeserialized,
            registered,
            false /* predefined */
        );

        // perform put() instead of putIfAbsent() because "registered" flag may have been changed.
        userTypes.put(typeId, desc);
        descByCls.put(cls, desc);

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

        return Set.class.isAssignableFrom(cls) ? GridPortableMarshaller.USER_SET : GridPortableMarshaller.USER_COL;
    }

    /**
     * @param cls Map class.
     * @return Map type ID.
     */
    public byte mapType(Class<? extends Map> cls) {
        assert cls != null;

        Byte type = mapTypes.get(cls);

        return type != null ? type : GridPortableMarshaller.USER_COL;
    }

    /**
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName) {
        String shortTypeName = typeName(typeName);

        Integer id = predefinedTypeNames.get(shortTypeName);

        if (id != null)
            return id;

        if (marshCtx.isSystemType(typeName))
            return typeName.hashCode();

        return idMapper(shortTypeName).typeId(shortTypeName);
    }

    /**
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    public int fieldId(int typeId, String fieldName) {
        return idMapper(typeId).fieldId(typeId, fieldName);
    }

    /**
     * @param typeId Type ID.
     * @return Instance of ID mapper.
     */
    public PortableIdMapper idMapper(int typeId) {
        PortableIdMapper idMapper = mappers.get(typeId);

        if (idMapper != null)
            return idMapper;

        if (userTypes.containsKey(typeId) || predefinedTypes.containsKey(typeId))
            return DFLT_ID_MAPPER;

        return BASIC_CLS_ID_MAPPER;
    }

    /**
     * @param typeName Type name.
     * @return Instance of ID mapper.
     */
    private PortableIdMapper idMapper(String typeName) {
        PortableIdMapper idMapper = typeMappers.get(typeName);

        return idMapper != null ? idMapper : DFLT_ID_MAPPER;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, gridName);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        gridName = U.readString(in);
    }

    /**
     * @return Portable context.
     * @throws ObjectStreamException In case of error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            IgniteKernal g = IgnitionEx.gridx(gridName);

            if (g == null)
                throw new IllegalStateException("Failed to find grid for name: " + gridName);

            return ((CacheObjectPortableProcessorImpl)g.context().cacheObjects()).portableContext();
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
    }

    /**
     * @param cls Class.
     * @param id Type ID.
     * @return GridPortableClassDescriptor.
     */
    public PortableClassDescriptor registerPredefinedType(Class<?> cls, int id) {
        String typeName = typeName(cls.getName());

        PortableClassDescriptor desc = new PortableClassDescriptor(
            this,
            cls,
            false,
            id,
            typeName,
            DFLT_ID_MAPPER,
            null,
            false,
            false,
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
     * @param useTs Use timestamp flag.
     * @param metaDataEnabled Metadata enabled flag.
     * @param keepDeserialized Keep deserialized flag.
     * @throws PortableException In case of error.
     */
    @SuppressWarnings("ErrorNotRethrown")
    public void registerUserType(String clsName,
        PortableIdMapper idMapper,
        @Nullable PortableSerializer serializer,
        @Nullable String affKeyFieldName,
        boolean useTs,
        boolean metaDataEnabled,
        boolean keepDeserialized)
        throws PortableException {
        assert idMapper != null;

        Class<?> cls = null;

        try {
            cls = Class.forName(clsName);
        }
        catch (ClassNotFoundException | NoClassDefFoundError ignored) {
            // No-op.
        }

        int id = idMapper.typeId(clsName);

        //Workaround for IGNITE-1358
        if (predefinedTypes.get(id) != null)
            throw new PortableException("Duplicate type ID [clsName=" + clsName + ", id=" + id + ']');

        if (mappers.put(id, idMapper) != null)
            throw new PortableException("Duplicate type ID [clsName=" + clsName + ", id=" + id + ']');

        if (useTs)
            usingTs.add(id);

        String typeName = typeName(clsName);

        typeMappers.put(typeName, idMapper);

        metaEnabled.put(id, metaDataEnabled);

        Map<String, String> fieldsMeta = null;

        if (cls != null) {
            PortableClassDescriptor desc = new PortableClassDescriptor(
                this,
                cls,
                true,
                id,
                typeName,
                idMapper,
                serializer,
                useTs,
                metaDataEnabled,
                keepDeserialized,
                true, /* registered */
                false /* predefined */
            );

            fieldsMeta = desc.fieldsMeta();

            userTypes.put(id, desc);
            descByCls.put(cls, desc);
        }

        metaHnd.addMeta(id, new PortableMetaDataImpl(typeName, fieldsMeta, affKeyFieldName));
    }

    /**
     * @param typeId Type ID.
     * @return Meta data.
     * @throws PortableException In case of error.
     */
    @Nullable public PortableMetadata metaData(int typeId) throws PortableException {
        return metaHnd != null ? metaHnd.metadata(typeId) : null;
    }

    /**
     * @param typeId Type ID.
     * @return Whether meta data is enabled.
     */
    public boolean isMetaDataEnabled(int typeId) {
        Boolean enabled = metaEnabled.get(typeId);

        return enabled != null ? enabled : metaDataEnabled;
    }

    /**
     * @param typeId Type ID.
     * @param metaHashSum Meta data hash sum.
     * @return Whether meta is changed.
     */
    boolean isMetaDataChanged(int typeId, @Nullable Integer metaHashSum) {
        if (metaHashSum == null)
            return false;

        Collection<Integer> hist = metaDataCache.get(typeId);

        if (hist == null) {
            Collection<Integer> old = metaDataCache.putIfAbsent(typeId, hist = new GridConcurrentHashSet<>());

            if (old != null)
                hist = old;
        }

        return hist.add(metaHashSum);
    }

    /**
     * @param typeId Type ID.
     * @param typeName Type name.
     * @param fields Fields map.
     * @throws PortableException In case of error.
     */
    public void updateMetaData(int typeId, String typeName, Map<String, String> fields) throws PortableException {
        updateMetaData(typeId, new PortableMetaDataImpl(typeName, fields, null));
    }

    /**
     * @param typeId Type ID.
     * @param meta Meta data.
     * @throws PortableException In case of error.
     */
    public void updateMetaData(int typeId, PortableMetaDataImpl meta) throws PortableException {
        metaHnd.addMeta(typeId, meta);
    }

    /**
     * @return Use timestamp flag.
     */
    public boolean isUseTimestamp() {
        return useTs;
    }

    /**
     * @param typeId Type ID.
     * @return If timestamp used.
     */
    public boolean isUseTimestamp(int typeId) {
        return usingTs.contains(typeId);
    }

    /**
     * @return Whether to convert string to UTF8 bytes.
     */
    public boolean isConvertString() {
        return convertStrings;
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
    public static String typeName(String clsName) {
        assert clsName != null;

        int idx = clsName.lastIndexOf('$');

        String typeName;

        if (idx >= 0) {
            typeName = clsName.substring(idx + 1);

            try {
                Integer.parseInt(typeName);

                // This is an anonymous class. Don't cut off enclosing class name for it.
                idx = -1;
            }
            catch (NumberFormatException e) {
                return typeName;
            }
        }

        if (idx < 0)
            idx = clsName.lastIndexOf('.');

        return idx >= 0 ? clsName.substring(idx + 1) : clsName;
    }

    /**
     * @param str String.
     * @return Hash code for given string converted to lower case.
     */
    private static int lowerCaseHashCode(String str) {
        int len = str.length();

        int h = 0;

        for (int i = 0; i < len; i++) {
            int c = str.charAt(i);

            c = c <= MAX_LOWER_CASE_CHAR ? LOWER_CASE_CHARS[c] : Character.toLowerCase(c);

            h = 31 * h + c;
        }

        return h;
    }

    /**
     */
    private static class IdMapperWrapper implements PortableIdMapper {
        /** */
        private final PortableIdMapper mapper;

        /**
         * @param mapper Custom ID mapper.
         */
        private IdMapperWrapper(@Nullable PortableIdMapper mapper) {
            this.mapper = mapper;
        }

        /** {@inheritDoc} */
        @Override public int typeId(String clsName) {
            int id = 0;

            if (mapper != null)
                id = mapper.typeId(clsName);

            return id != 0 ? id : lowerCaseHashCode(typeName(clsName));
        }

        /** {@inheritDoc} */
        @Override public int fieldId(int typeId, String fieldName) {
            int id = 0;

            if (mapper != null)
                id = mapper.fieldId(typeId, fieldName);

            return id != 0 ? id : lowerCaseHashCode(fieldName);
        }
    }

    private static class BasicClassIdMapper implements PortableIdMapper {
        /** {@inheritDoc} */
        @Override public int typeId(String clsName) {
            return clsName.hashCode();
        }

        /** {@inheritDoc} */
        @Override public int fieldId(int typeId, String fieldName) {
            return lowerCaseHashCode(fieldName);
        }
    }

    /**
     * Type descriptors.
     */
    private static class TypeDescriptors {
        /** Descriptors map. */
        private final Map<String, TypeDescriptor> descs = new HashMap<>();

        /**
         * Add type descriptor.
         *
         * @param clsName Class name.
         * @param idMapper ID mapper.
         * @param serializer Serializer.
         * @param affKeyFieldName Affinity key field name.
         * @param useTs Use timestamp flag.
         * @param metadataEnabled Metadata enabled flag.
         * @param keepDeserialized Keep deserialized flag.
         * @param canOverride Whether this descriptor can be override.
         * @throws PortableException If failed.
         */
        private void add(String clsName,
            PortableIdMapper idMapper,
            PortableSerializer serializer,
            String affKeyFieldName,
            boolean useTs,
            boolean metadataEnabled,
            boolean keepDeserialized,
            boolean canOverride)
            throws PortableException {
            TypeDescriptor desc = new TypeDescriptor(clsName,
                idMapper,
                serializer,
                affKeyFieldName,
                useTs,
                metadataEnabled,
                keepDeserialized,
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
        private PortableIdMapper idMapper;

        /** Serializer. */
        private PortableSerializer serializer;

        /** Affinity key field name. */
        private String affKeyFieldName;

        /** Use timestamp flag. */
        private boolean useTs;

        /** Metadata enabled flag. */
        private boolean metadataEnabled;

        /** Keep deserialized flag. */
        private boolean keepDeserialized;

        /** Whether this descriptor can be override. */
        private boolean canOverride;

        /**
         * Constructor.
         *
         * @param clsName Class name.
         * @param idMapper ID mapper.
         * @param serializer Serializer.
         * @param affKeyFieldName Affinity key field name.
         * @param useTs Use timestamp flag.
         * @param metadataEnabled Metadata enabled flag.
         * @param keepDeserialized Keep deserialized flag.
         * @param canOverride Whether this descriptor can be override.
         */
        private TypeDescriptor(String clsName, PortableIdMapper idMapper, PortableSerializer serializer,
            String affKeyFieldName, boolean useTs, boolean metadataEnabled, boolean keepDeserialized,
            boolean canOverride) {
            this.clsName = clsName;
            this.idMapper = idMapper;
            this.serializer = serializer;
            this.affKeyFieldName = affKeyFieldName;
            this.useTs = useTs;
            this.metadataEnabled = metadataEnabled;
            this.keepDeserialized = keepDeserialized;
            this.canOverride = canOverride;
        }

        /**
         * Override portable class descriptor.
         *
         * @param other Other descriptor.
         * @throws PortableException If failed.
         */
        private void override(TypeDescriptor other) throws PortableException {
            assert clsName.equals(other.clsName);

            if (canOverride) {
                idMapper = other.idMapper;
                serializer = other.serializer;
                affKeyFieldName = other.affKeyFieldName;
                useTs = other.useTs;
                metadataEnabled = other.metadataEnabled;
                keepDeserialized = other.keepDeserialized;
                canOverride = other.canOverride;
            }
            else if (!other.canOverride)
                throw new PortableException("Duplicate explicit class definition in configuration: " + clsName);
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