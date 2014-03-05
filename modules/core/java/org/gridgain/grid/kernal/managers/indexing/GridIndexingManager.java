/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.indexing;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Manages cache indexing.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridIndexingManager extends GridManagerAdapter<GridIndexingSpi> {
    /** */
    private GridMarshaller marsh;

    /** Type descriptors. */
    private final ConcurrentMap<TypeId, TypeDescriptor> types = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<Long, ClassLoader> ldrById = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<ClassLoader, Long> idByLdr = new ConcurrentHashMap8<>();

    /** */
    private final AtomicLong ldrIdGen = new AtomicLong();

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** */
    private ExecutorService execSvc;

    /**
     * @param ctx  Kernal context.
     */
    public GridIndexingManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getIndexingSpi());

        marsh = ctx.config().getMarshaller();
    }

    /**
     * @throws GridException Thrown in case of any errors.
     */
    @Override public void start() throws GridException {
        if (ctx.config().isDaemon())
            return;

        GridIndexingMarshaller m = new IdxMarshaller();

        for (GridIndexingSpi spi : getSpis()) {
            spi.registerMarshaller(m);

            for (GridCacheConfiguration cacheCfg : ctx.config().getCacheConfiguration())
                spi.registerSpace(cacheCfg.getName());
        }

        execSvc = ctx.config().getExecutorService();

        startSpi();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        if (ctx.config().isDaemon())
            return;

        busyLock.block();
    }

    /**
     * @throws GridException Thrown in case of any errors.
     */
    @Override public void stop(boolean cancel) throws GridException {
        if (ctx.config().isDaemon())
            return;

        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Returns number of objects of given type for given space of spi.
     *
     * @param spi SPI Name.
     * @param space Space.
     * @param valType Value type.
     * @return Objects number or -1 if this type is unknown for given SPI and space.
     * @throws GridException If failed.
     */
    public long size(@Nullable String spi, @Nullable String space, Class<?> valType) throws GridException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to get space size (grid is stopping).");

        try {
            TypeDescriptor desc = types.get(new TypeId(space, valType));

            if (desc == null || !desc.registered())
                return -1;

            return getSpi(spi).size(space, desc);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Rebuilds all search indexes of given value type for given space of spi.
     *
     * @param spi SPI name.
     * @param space Space.
     * @param valType Value type.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    public GridFuture<?> rebuildIndexes(@Nullable final String spi, @Nullable final String space, Class<?> valType) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to rebuild indexes (grid is stopping).");

        try {
            final TypeDescriptor desc = types.get(new TypeId(space, valType));

            if (desc == null || !desc.registered())
                return new GridFinishedFuture<Void>(ctx);

            final GridWorkerFuture<?> fut = new GridWorkerFuture<Void>();

            GridWorker w = new GridWorker(ctx.gridName(), "index-rebuild-worker", log) {
                @Override protected void body() {
                    try {
                        getSpi(spi).rebuildIndexes(space, desc);

                        fut.onDone();
                    }
                    catch (Exception e) {
                        fut.onDone(e);
                    }
                    catch (Throwable e) {
                        log.error("Failed to rebuild indexes for type: " + desc.name(), e);

                        fut.onDone(e);
                    }
                }
            };

            fut.setWorker(w);

            execSvc.execute(w);

            return fut;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Rebuilds all search indexes for given spi.
     *
     * @param spi SPI name.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    @SuppressWarnings("unchecked")
    public GridFuture<?> rebuildAllIndexes(@Nullable String spi) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to get space size (grid is stopping).");

        try {
            GridCompoundFuture<?, ?> fut = new GridCompoundFuture<Object, Object>(ctx);

            for (TypeId type : types.keySet())
                fut.add((GridFuture)rebuildIndexes(spi, type.space, type.valType));

            fut.markInitialized();

            return fut;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * FOR TESTING ONLY
     *
     * @param name SPI Name.
     * @return SPI.
     */
    public GridIndexingSpi spi(@Nullable String name) {
        if (F.isEmpty(name))
            return getSpis()[0];

        for (GridIndexingSpi s : getSpis()) {
            if (name.equals(s.getName()))
                return s;
        }

        throw new GridRuntimeException("Failed to find SPI for name: " + name);
    }

    /**
     * @param x Value.
     * @param bytes Serialized value.
     * @param <X> Value type.
     * @return Index entry.
     */
    private <X> GridIndexingEntity<X> entry(X x, @Nullable byte[] bytes) {
        return new GridIndexingEntityAdapter<>(x, bytes);
    }

    /**
     * Writes key-value pair to index.
     *
     * @param spi SPI Name.
     * @param space Space.
     * @param key Key.
     * @param keyBytes Byte array with key data.
     * @param val Value.
     * @param valBytes Byte array with value data.
     * @param ver Cache entry version.
     * @param expirationTime Expiration time or 0 if never expires.
     * @throws GridException In case of error.
     */
    @SuppressWarnings("unchecked")
    public <K, V> void store(final String spi, final String space, final K key, @Nullable byte[] keyBytes, V val,
        @Nullable byte[] valBytes, byte[] ver, long expirationTime) throws GridException {
        assert key != null;
        assert val != null;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to write to index (grid is stopping).");

        try {
            if (log.isDebugEnabled())
                log.debug("Storing key to cache query index [key=" + key + ", value=" + val + "]");

            final Class<?> valCls = val.getClass();

            TypeId id = new TypeId(space, valCls);

            TypeDescriptor desc = types.get(id);

            if (desc == null) {
                desc = new TypeDescriptor();

                TypeDescriptor existing = types.putIfAbsent(id, desc);

                if (existing != null)
                    desc = existing;
            }

            if (!desc.succeeded()) {
                final TypeDescriptor d = desc;

                d.init(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        String typeName = valCls.getSimpleName();

                        // To protect from failure on anonymous classes.
                        if (F.isEmpty(typeName))
                            typeName = valCls.getName().substring(valCls.getPackage().getName().length());

                        if (valCls.isArray()) {
                            assert typeName.endsWith("[]");

                            typeName = typeName.substring(0, typeName.length() - 2) + "_array";
                        }

                        d.name(typeName);

                        d.keyClass(key.getClass());
                        d.valueClass(valCls);

                        processAnnotationsInClass(true, d.keyCls, d, null);
                        processAnnotationsInClass(false, d.valCls, d, null);

                        d.registered(getSpi(spi).registerType(space, d));

                        return null;
                    }
                });
            }

            if (!desc.registered())
                return;

            GridIndexingEntity<K> k = entry(key, keyBytes);
            GridIndexingEntity<V> v = entry(val, valBytes);

            getSpi(spi).store(space, desc, k, v, ver, expirationTime);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param spi SPI Name.
     * @param space Space.
     * @param key Key.
     * @param keyBytes Byte array with key value.
     * @return {@code true} if key was found and removed, otherwise {@code false}.
     * @throws GridException Thrown in case of any errors.
     */
    @SuppressWarnings("unchecked")
    public <K, V> boolean remove(String spi, String space, K key, @Nullable byte[] keyBytes) throws GridException {
        assert key != null;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to remove from index (grid is stopping).");

        try {
            GridIndexingEntity<K> k = entry(key, keyBytes);

            return getSpi(spi).remove(space, k);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param spi SPI Name.
     * @param space Space name.
     * @param clause Clause.
     * @param params Parameters collection.
     * @param includeBackups Include or exclude backup entries.
     * @param filters Key and value filters.
     * @return Field rows.
     * @throws GridException If failed.
     */
    public <K, V> GridIndexingFieldsResult queryFields(@Nullable String spi, @Nullable String space,
        String clause, Collection<Object> params, boolean includeBackups,
        GridIndexingQueryFilter<K, V>... filters) throws GridException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            GridIndexingQueryFilter<K, V> backupFilter = backupsFilter(space, includeBackups);

            return getSpi(spi).queryFields(space, clause, params,
                backupFilter != null ? F.concat(filters, backupFilter) : filters);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param spi SPI Name.
     * @param space Space.
     * @param clause Clause.
     * @param params Parameters collection.
     * @param resType Result type.
     * @param includeBackups Include or exclude backup entries.
     * @param filters Filters.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Key/value rows.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCloseableIterator<GridIndexingKeyValueRow<K, V>> query(String spi, String space, String clause,
        Collection<Object> params, Class<? extends V> resType, boolean includeBackups,
        GridIndexingQueryFilter<K, V>... filters) throws GridException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            TypeDescriptor type = types.get(new TypeId(space, resType));

            if (type == null || !type.registered())
                return new GridEmptyCloseableIterator<>();

            GridIndexingQueryFilter<K, V> backupFilter = backupsFilter(space, includeBackups);

            return new GridSpiCloseableIteratorWrapper<>(getSpi(spi).query(space, clause, params, type,
                backupFilter != null ? F.concat(filters, backupFilter) : filters));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param spi SPI Name.
     * @param space Space.
     * @param clause Clause.
     * @param resType Result type.
     * @param includeBackups Include or exclude backup entries.
     * @param filters Key and value filters.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Key/value rows.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCloseableIterator<GridIndexingKeyValueRow<K, V>> queryText(String spi, String space,
        String clause, Class<? extends V> resType, boolean includeBackups,
        GridIndexingQueryFilter<K, V>... filters) throws GridException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            TypeDescriptor type = types.get(new TypeId(space, resType));

            if (type == null || !type.registered())
                return new GridEmptyCloseableIterator<>();

            GridIndexingQueryFilter<K, V> backupFilter = backupsFilter(space, includeBackups);

            return new GridSpiCloseableIteratorWrapper<>(getSpi(spi).queryText(space, clause, type,
                backupFilter != null ? F.concat(filters, backupFilter) : filters));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Predicate.
     * @param spaceName Space name.
     * @param includeBackups Include backups.
     */
    @SuppressWarnings("unchecked")
    @Nullable private <K, V> GridIndexingQueryFilter<K, V> backupsFilter(String spaceName, boolean includeBackups) {
        if (includeBackups || ctx.cache().internalCache(spaceName).context().isReplicated())
            return null;

        final UUID nodeId = ctx.localNodeId();

        return new GridIndexingQueryFilter<K, V>() {
            @Override public boolean apply(String spaceName, K key, V val) {
                try {
                    return ctx.cache().internalCache(spaceName).context().isReplicated() ||
                        nodeId.equals(ctx.affinity().mapKeyToNode(spaceName, key).id());
                }
                catch (GridException e) {
                    throw F.wrap(e);
                }
            }
        };
    }

    /**
     * Will be called when entry for key will be swapped.
     *
     * @param spi Spi name.
     * @param spaceName Space name.
     * @param swapSpaceName Swap space name.
     * @param key key.
     * @throws GridSpiException If failed.
     */
    public void onSwap(String spi, String spaceName, String swapSpaceName, Object key) throws GridSpiException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process swap event (grid is stopping).");

        try {
            getSpi(spi).onSwap(spaceName, swapSpaceName, key);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Will be called when entry for key will be unswapped.
     *
     * @param spi Spi name.
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @param valBytes Value bytes.
     * @throws GridSpiException If failed.
     */
    public void onUnswap(String spi, String spaceName, Object key, Object val, byte[] valBytes)
        throws GridSpiException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process swap event (grid is stopping).");

        try {
            getSpi(spi).onUnswap(spaceName, key, val, valBytes);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Removes index tables for all classes belonging to given class loader.
     *
     * @param space Space name.
     * @param ldr Class loader to undeploy.
     * @throws GridException If undeploy failed.
     */
    public void onUndeploy(@Nullable String space, ClassLoader ldr) throws GridException {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process undeploy event (grid is stopping).");

        try {
            try {
                Iterator<Map.Entry<TypeId, TypeDescriptor>> it = types.entrySet().iterator();

                while (it.hasNext()) {
                    Map.Entry<TypeId, TypeDescriptor> e = it.next();

                    if (!F.eq(e.getKey().space, space))
                        continue;

                    TypeDescriptor desc = e.getValue();

                    if (ldr.equals(U.detectClassLoader(desc.valCls)) || ldr.equals(U.detectClassLoader(desc.keyCls))) {
                        for (GridIndexingSpi spi : getSpis()) {
                            if (desc.await() && desc.registered())
                                spi.unregisterType(e.getKey().space, desc);
                        }

                        it.remove();
                    }
                }
            }
            finally {
                Long id = idByLdr.remove(ldr);

                if (id != null)
                    ldrById.remove(id);
            }
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Process annotations for class.
     *
     * @param key If given class relates to key.
     * @param cls Class.
     * @param type Type descriptor.
     * @param parent Parent in case of embeddable.
     * @throws GridException In case of error.
     */
    static void processAnnotationsInClass(boolean key, Class<?> cls, @Nullable TypeDescriptor type,
        @Nullable Property parent) throws GridException {
        if (U.isJdk(cls))
            return;

        if (parent != null && parent.knowsClass(cls))
            throw new GridException("Recursive reference found in type: " + cls.getName());

        if (parent == null) { // Check class annotation at top level only.
            GridCacheQueryTextField txtAnnCls = cls.getAnnotation(GridCacheQueryTextField.class);

            if (txtAnnCls != null)
                type.valueTextIndex(true);

            GridCacheQueryGroupIndex grpIdx = cls.getAnnotation(GridCacheQueryGroupIndex.class);

            if (grpIdx != null)
                type.addIndex(grpIdx.name(), grpIdx.unique());

            GridCacheQueryGroupIndex.List grpIdxList = cls.getAnnotation(GridCacheQueryGroupIndex.List.class);

            if (grpIdxList != null && !F.isEmpty(grpIdxList.value())) {
                for (GridCacheQueryGroupIndex idx : grpIdxList.value())
                    type.addIndex(idx.name(), idx.unique());
            }
        }

        for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                GridCacheQuerySqlField sqlAnn = field.getAnnotation(GridCacheQuerySqlField.class);
                GridCacheQueryTextField txtAnn = field.getAnnotation(GridCacheQueryTextField.class);

                if (sqlAnn != null || txtAnn != null) {
                    Property prop = new Property(field);

                    prop.parent(parent);

                    processAnnotation(key, sqlAnn, txtAnn, field.getType(), prop, type);

                    type.addProperty(key, prop);
                }
            }

            for (Method mtd : c.getDeclaredMethods()) {
                GridCacheQuerySqlField sqlAnn = mtd.getAnnotation(GridCacheQuerySqlField.class);
                GridCacheQueryTextField txtAnn = mtd.getAnnotation(GridCacheQueryTextField.class);

                if (sqlAnn != null || txtAnn != null) {
                    if (mtd.getParameterTypes().length != 0)
                        throw new GridException("Getter with GridCacheQuerySqlField " +
                            "annotation cannot have parameters: " + mtd);

                    Property prop = new Property(mtd);

                    prop.parent(parent);

                    processAnnotation(key, sqlAnn, txtAnn, mtd.getReturnType(), prop, type);

                    type.addProperty(key, prop);
                }
            }
        }
    }

    /**
     * Processes annotation at field or method.
     *
     * @param key If given class relates to key.
     * @param sqlAnn SQL annotation, can be {@code null}.
     * @param txtAnn H2 text annotation, can be {@code null}.
     * @param cls Class of field or return type for method.
     * @param prop Current property.
     * @param desc Class description.
     * @throws GridException In case of error.
     */
    static void processAnnotation(boolean key, GridCacheQuerySqlField sqlAnn, GridCacheQueryTextField txtAnn,
        Class<?> cls, Property prop, TypeDescriptor desc) throws GridException {
        if (sqlAnn != null) {
            processAnnotationsInClass(key, cls, desc, prop);

            if (!sqlAnn.name().isEmpty())
                prop.name(sqlAnn.name());

            if (sqlAnn.index() || sqlAnn.unique()) {
                String idxName = prop.name() + "_idx";

                desc.addIndex(idxName, sqlAnn.unique());

                desc.addFieldToIndex(idxName, prop.name(), 0, sqlAnn.descending());
            }

            if (!F.isEmpty(sqlAnn.groups())) {
                for (String group : sqlAnn.groups())
                    desc.addFieldToIndex(group, prop.name(), 0, false);
            }

            if (!F.isEmpty(sqlAnn.orderedGroups())) {
                for (GridCacheQuerySqlField.Group idx : sqlAnn.orderedGroups())
                    desc.addFieldToIndex(idx.name(), prop.name(), idx.order(), idx.descending());
            }
        }

        if (txtAnn != null)
            desc.addFieldToTextIndex(prop.name());
    }

    /**
     * Gets types for space.
     *
     * @param space Space name.
     * @return Descriptors.
     */
    public Collection<GridIndexingTypeDescriptor> types(@Nullable String space) {
        Collection<GridIndexingTypeDescriptor> spaceTypes = new ArrayList<>(
            Math.min(10, types.size()));

        for (Map.Entry<TypeId, TypeDescriptor> e : types.entrySet()) {
            TypeDescriptor desc = e.getValue();

            if (desc.registered() && F.eq(e.getKey().space, space))
                spaceTypes.add(desc);
        }

        return spaceTypes;
    }

    /**
     * Description of type property.
     */
    private static class Property {
        /** */
        private final Member member;

        /** */
        private Property parent;

        /** */
        private String name;

        /**
         * Constructor.
         *
         * @param member Element.
         */
        Property(Member member) {
            this.member = member;

            name = member instanceof Method && member.getName().startsWith("get") && member.getName().length() > 3 ?
                member.getName().substring(3) : member.getName();
        }

        /**
         * Gets this property value from the given object.
         *
         * @param x Object with this property.
         * @return Property value.
         * @throws GridSpiException If failed.
         */
        public Object value(Object x) throws GridSpiException {
            if (parent != null)
                x = parent.value(x);

            if (x == null)
                return null;

            try {
                if (member instanceof Field) {
                    Field field = (Field)member;

                    field.setAccessible(true);

                    return field.get(x);
                }
                else {
                    Method mtd = (Method)member;

                    mtd.setAccessible(true);

                    return mtd.invoke(x);
                }
            }
            catch (Exception e) {
                throw new GridSpiException(e);
            }
        }

        /**
         * @param name Property name.
         */
        public void name(String name) {
            this.name = name;
        }

        /**
         * @return Property name.
         */
        public String name() {
            return name;
        }

        /**
         * @return Class member type.
         */
        public Class<?> type() {
            return member instanceof Field ? ((Field)member).getType() : ((Method)member).getReturnType();
        }

        /**
         * @param parent Parent property if this is embeddable element.
         */
        public void parent(Property parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Property.class, this);
        }

        /**
         * @param cls Class.
         * @return {@code true} If this property or some parent relates to member of the given class.
         */
        public boolean knowsClass(Class<?> cls) {
            return member.getDeclaringClass() == cls || (parent != null && parent.knowsClass(cls));
        }
    }

    /**
     * Descriptor of type.
     */
    private static class TypeDescriptor implements GridIndexingTypeDescriptor {
        /** */
        private String name;

        /** Value field names and types with preserved order. */
        @GridToStringInclude
        private final Map<String, Class<?>> valFields = new LinkedHashMap<>();

        /** */
        @GridToStringExclude
        private final Map<String, Property> props = new HashMap<>();

        /** Key field names and types with preserved order. */
        @GridToStringInclude
        private final Map<String, Class<?>> keyFields = new LinkedHashMap<>();

        /** */
        @GridToStringInclude
        private final Map<String, IndexDescriptor> indexes = new HashMap<>();

        /** */
        private IndexDescriptor fullTextIdx;

        /** */
        private Class<?> keyCls;

        /** */
        private Class<?> valCls;

        /** */
        private boolean valTextIdx;

        /** To ensure that type was registered in SPI and only once. */
        private final GridAtomicInitializer<Void> initializer = new GridAtomicInitializer<>();

        /** SPI can decide not to register this type. */
        private boolean registered;

        /**
         * @param c Initialization callable.
         * @throws GridException In case of error.
         */
        void init(Callable<Void> c) throws GridException {
            initializer.init(c);
        }

        /**
         * @return Waits for initialization.
         * @throws GridInterruptedException If thread is interrupted.
         */
        boolean await() throws GridInterruptedException {
            return initializer.await();
        }

        /**
         * @return Whether initialization was successfully completed.
         */
        boolean succeeded() {
            return initializer.succeeded();
        }

        /**
         * @return {@code True} if type registration in SPI was finished and type was not rejected.
         */
        boolean registered() {
            return initializer.succeeded() && registered;
        }

        /**
         * @param registered Sets registered flag.
         */
        void registered(boolean registered) {
            this.registered = registered;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /**
         * Sets type name.
         *
         * @param name Name.
         */
        void name(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Class<?>> valueFields() {
            return valFields;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Class<?>> keyFields() {
            return keyFields;
        }

        /** {@inheritDoc} */
        @Override public <T> T value(Object obj, String field) throws GridSpiException {
            assert obj != null;
            assert field != null;

            Property prop = props.get(field);

            if (prop == null)
                throw new GridSpiException("Failed to find field '" + field + "' in type '" + name + "'.");

            return (T)prop.value(obj);
        }

        /** {@inheritDoc} */
        @Override public Map<String, GridIndexDescriptor> indexes() {
            return Collections.<String, GridIndexDescriptor>unmodifiableMap(indexes);
        }

        /**
         * Adds index.
         *
         * @param idxName Index name.
         * @param unique Unique index.
         * @return Index descriptor.
         * @throws GridException In case of error.
         */
        public IndexDescriptor addIndex(String idxName, boolean unique) throws GridException {
            IndexDescriptor idx = new IndexDescriptor(unique, false);

            if (indexes.put(idxName, idx) != null)
                throw new GridException("Index with name '" + idxName + "' already exists.");

            return idx;
        }

        /**
         * Adds field to index.
         *
         * @param idxName Index name.
         * @param field Field name.
         * @param orderNum Fields order number in index.
         * @param descending Sorting order.
         * @throws GridException If failed.
         */
        public void addFieldToIndex(String idxName, String field, int orderNum,
            boolean descending) throws GridException {
            IndexDescriptor desc = indexes.get(idxName);

            if (desc == null)
                desc = addIndex(idxName, false);

            desc.addField(field, orderNum, descending);
        }

        /**
         * Adds field to text index.
         *
         * @param field Field name.
         */
        public void addFieldToTextIndex(String field) {
            if (fullTextIdx == null) {
                fullTextIdx = new IndexDescriptor(false, true);

                indexes.put(null, fullTextIdx);
            }

            fullTextIdx.addField(field, 0, false);
        }

        /** {@inheritDoc} */
        @Override public Class<?> valueClass() {
            return valCls;
        }

        /**
         * Sets value class.
         *
         * @param valCls Value class.
         */
        void valueClass(Class<?> valCls) {
            this.valCls = valCls;
        }

        /** {@inheritDoc} */
        @Override public Class<?> keyClass() {
            return keyCls;
        }

        /**
         * Set key class.
         *
         * @param keyCls Key class.
         */
        void keyClass(Class<?> keyCls) {
            this.keyCls = keyCls;
        }

        /**
         * Adds property to the type descriptor.
         *
         * @param key If given property relates to key.
         * @param prop Property.
         * @throws GridException In case of error.
         */
        public void addProperty(boolean key, Property prop) throws GridException {
            String name = prop.name();

            if (props.put(name, prop) != null)
                throw new GridException("Property with name '" + name + "' already exists.");

            if (key)
                keyFields.put(name, prop.type());
            else
                valFields.put(name, prop.type());
        }

        /** {@inheritDoc} */
        @Override public boolean valueTextIndex() {
            return valTextIdx;
        }

        /**
         * Sets if this value should be text indexed.
         *
         * @param valTextIdx Flag value.
         */
        public void valueTextIndex(boolean valTextIdx) {
            this.valTextIdx = valTextIdx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TypeDescriptor.class, this);
        }
    }

    /**
     * Index descriptor.
     */
    private static class IndexDescriptor implements GridIndexDescriptor {
        /** Fields sorted by order number. */
        private final Collection<T2<String, Integer>> fields = new TreeSet<>(
            new Comparator<T2<String, Integer>>() {
                @Override public int compare(T2<String, Integer> o1, T2<String, Integer> o2) {
                    return o1.get2() - o2.get2();
                }
            });

        /** Fields which should be indexed in descending order. */
        private Collection<String> descendings;

        /** */
        private final boolean unique;

        /** */
        private final boolean text;

        /**
         * @param unique Unique index.
         * @param text Text index.
         */
        private IndexDescriptor(boolean unique, boolean text) {
            this.unique = unique;
            this.text = text;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> fields() {
            Collection<String> res = new ArrayList<>(fields.size());

            for (T2<String, Integer> t : fields)
                res.add(t.get1());

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean descending(String field) {
            return descendings != null && descendings.contains(field);
        }

        /**
         * Adds field to this index.
         *
         * @param field Field name.
         * @param orderNum Field order number in this index.
         * @param descending Sort order.
         */
        public void addField(String field, int orderNum, boolean descending) {
            fields.add(new T2<>(field, orderNum));

            if (descending) {
                if (descendings == null)
                    descendings  = new HashSet<>();

                descendings.add(field);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean unique() {
            return unique;
        }

        /** {@inheritDoc} */
        @Override public boolean text() {
            return text;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexDescriptor.class, this);
        }
    }

    /**
     * Identifying TypeDescriptor by space and value class.
     */
    private static class TypeId {
        /** */
        private final String space;

        /** */
        private final Class<?> valType;

        /**
         * Constructor.
         *
         * @param space Space name.
         * @param valType Value type.
         */
        private TypeId(String space, Class<?> valType) {
            assert valType != null;

            this.space = space;
            this.valType = valType;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TypeId typeId = (TypeId)o;

            return (space != null ? space.equals(typeId.space) : typeId.space == null) &&
                valType.equals(typeId.valType);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 31 * (space != null ? space.hashCode() : 0) + valType.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TypeId.class, this);
        }
    }

    /**
     * Indexing marshaller which also stores information about class loader and allows lazy unmarshalling.
     */
    private class IdxMarshaller implements GridIndexingMarshaller {
        /** {@inheritDoc} */
        @Override public <T> GridIndexingEntity<T> unmarshal(final byte[] bytes) {
            long ldrId = bytes[0] == -1 ? 0 : U.bytesToLong(bytes, 0);

            final ClassLoader ldr = ldrId == 0 ? null : ldrById.get(ldrId);

            final int off = ldrId == 0 ? 1 : 8;

            final int len = bytes.length - off;

            return new GridIndexingEntity<T>() {
                /** */
                private T val;

                /** */
                private byte[] valBytes;

                @Override public T value() throws GridSpiException {
                    if (val == null) {
                        try {
                            val = marsh.unmarshal(new ByteArrayInputStream(bytes, off, len), ldr);
                        }
                        catch (GridException e) {
                            throw new GridSpiException(e);
                        }
                    }

                    return val;
                }

                @Override public byte[] bytes() {
                    if (valBytes == null) {
                        byte[] bs = new byte[len];

                        U.arrayCopy(bytes, off, bs, 0, len);

                        valBytes = bs;
                    }

                    return valBytes;
                }

                @Override public boolean hasValue() {
                    return val != null;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public byte[] marshal(GridIndexingEntity<?> entity) throws GridSpiException {
            Object val = entity.value();

            ClassLoader ldr = val.getClass().getClassLoader();

            byte[] bytes = entity.bytes();

            ByteArrayOutputStream out = new ByteArrayOutputStream(bytes == null ? 128 : bytes.length + 8);

            if (ldr == null)
                // In special case of bootstrap class loader ldrId will be one byte -1.
                out.write(-1);
            else {
                Long ldrId;

                while ((ldrId = idByLdr.get(ldr)) == null) {
                    ldrId = ldrIdGen.incrementAndGet();

                    if (idByLdr.putIfAbsent(ldr, ldrId) == null) {
                        ldrById.put(ldrId, ldr);

                        break;
                    }
                }

                try {
                    out.write(U.longToBytes(ldrId));
                }
                catch (IOException e) {
                    throw new GridSpiException(e);
                }
            }

            try {
                if (bytes == null)
                    marsh.marshal(val, out);
                else
                    out.write(bytes);
            }
            catch (Exception e) {
                throw new GridSpiException(e);
            }

            return out.toByteArray();
        }
    }
}
