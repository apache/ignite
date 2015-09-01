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

package org.apache.ignite.internal.managers.deployment;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicStampedReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * Represents single class deployment.
 */
public class GridDeployment extends GridMetadataAwareAdapter implements GridDeploymentInfo {
    /** Timestamp. */
    private final long ts = U.currentTimeMillis();

    /** Deployment mode. */
    private final DeploymentMode depMode;

    /** Class loader. */
    private final ClassLoader clsLdr;

    /** Class loader ID. */
    private final IgniteUuid clsLdrId;

    /** User version. */
    private final String userVer;

    /** Flag indicating local (non-p2p) deployment. */
    private final boolean loc;

    /** Sample class name.*/
    private final String sampleClsName;

    /** {@code True} if undeploy was scheduled. */
    private volatile boolean pendingUndeploy;

    /** Undeployed flag and current usage count. */
    @GridToStringExclude
    private final AtomicStampedReference<Boolean> usage = new AtomicStampedReference<>(false, 0);

    /** Class annotations. */
    @GridToStringExclude
    private final ConcurrentMap<Class<?>,
        ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>>> anns =
        new ConcurrentHashMap8<>();

    /** Classes. */
    @GridToStringExclude
    private final ConcurrentMap<String, Class<?>> clss = new ConcurrentHashMap8<>();

    /** Task classes 'internal' flags. */
    @GridToStringExclude
    private final ConcurrentMap<Class<?>, Boolean> internalTasks = new ConcurrentHashMap8<>();

    /** Field cache. */
    @GridToStringExclude
    private final ConcurrentMap<Class<?>, ConcurrentMap<Class<? extends Annotation>, Collection<Field>>> fieldCache =
        new ConcurrentHashMap8<>();

    /** Method cache. */
    @GridToStringExclude
    private final ConcurrentMap<Class<?>, ConcurrentMap<Class<? extends Annotation>, Collection<Method>>> mtdCache =
        new ConcurrentHashMap8<>();

    /** Default constructor cache. */
    @GridToStringExclude
    private final ConcurrentMap<Class<?>, GridTuple<Constructor<?>>> dfltCtorsCache =
        new ConcurrentHashMap8<>();

    /**
     * @param depMode Deployment mode.
     * @param clsLdr Class loader.
     * @param clsLdrId Class loader ID.
     * @param userVer User version.
     * @param sampleClsName Sample class name.
     * @param loc {@code True} if local deployment.
     */
    GridDeployment(DeploymentMode depMode, ClassLoader clsLdr, IgniteUuid clsLdrId, String userVer,
        String sampleClsName, boolean loc) {
        assert depMode != null;
        assert clsLdr != null;
        assert clsLdrId != null;
        assert userVer != null;
        assert sampleClsName != null;

        this.clsLdr = clsLdr;
        this.clsLdrId = clsLdrId;
        this.userVer = userVer;
        this.depMode = depMode;
        this.sampleClsName = sampleClsName;
        this.loc = loc;
    }

    /**
     * Gets timestamp.
     *
     * @return Timestamp.
     */
    public long timestamp() {
        return ts;
    }

    /**
     * @return Sample class name.
     */
    public String sampleClassName() {
        return sampleClsName;
    }

    /**
     * Gets property depMode.
     *
     * @return Property depMode.
     */
    @Override public DeploymentMode deployMode() {
        return depMode;
    }

    /** {@inheritDoc} */
    @Override public boolean localDeploymentOwner() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long sequenceNumber() {
        return clsLdrId.localId();
    }

    /**
     * @return Class loader.
     */
    public ClassLoader classLoader() {
        return clsLdr;
    }

    /**
     * Gets property clsLdrId.
     *
     * @return Property clsLdrId.
     */
    @Override public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * Gets property userVer.
     *
     * @return Property userVer.
     */
    @Override public String userVersion() {
        return userVer;
    }

    /**
     * @param name Either class name or alias.
     * @return {@code True} if name is equal to either class name or alias.
     */
    public boolean hasName(String name) {
        assert name != null;

        return clss.containsKey(name);
    }

    /**
     * Gets property local.
     *
     * @return Property local.
     */
    public boolean local() {
        return loc;
    }

    /**
     * Gets property undeployed.
     *
     * @return Property undeployed.
     */
    public boolean undeployed() {
        return usage.getReference();
    }

    /**
     * Sets property undeployed.
     */
    public void undeploy() {
        int[] stamp = new int[1];

        while (true) {
            boolean undeployed = usage.get(stamp);

            if (undeployed)
                return;

            int r = stamp[0];

            if (usage.compareAndSet(false, true, r, r))
                return;
        }
    }

    /**
     * Gets property pendingUndeploy.
     *
     * @return Property pendingUndeploy.
     */
    public boolean pendingUndeploy() {
        return pendingUndeploy;
    }

    /**
     * Invoked whenever this deployment is scheduled to be undeployed.
     * Used for handling obsolete or phantom requests.
     */
    public void onUndeployScheduled() {
        pendingUndeploy = true;
    }

    /**
     * Increments usage count for deployment. If deployment is undeployed,
     * then usage count is not incremented.
     *
     * @return {@code True} if deployment is still active.
     */
    public boolean acquire() {
        int[] stamp = new int[1];

        while (true) {
            boolean undeployed = usage.get(stamp);

            int r = stamp[0];

            if (undeployed && r == 0)
                // Obsolete deployment.
                return false;

            if (usage.compareAndSet(undeployed, undeployed, r, r + 1))
                return true;
        }
    }

    /**
     * Decrements usage count.
     */
    public void release() {
        int[] stamp = new int[1];

        while (true) {
            boolean undeployed = usage.get(stamp);

            int r = stamp[0];

            assert r > 0 : "Invalid usages count: " + r;

            if (usage.compareAndSet(undeployed, undeployed, r, r - 1))
                return;
        }
    }

    /**
     * Checks if deployment is obsolete, i.e. is not used and has been undeployed.
     *
     * @return {@code True} if deployment is obsolete.
     */
    public boolean obsolete() {
        int[] stamp = new int[1];

        boolean undeployed = usage.get(stamp);

        return undeployed && stamp[0] == 0;
    }

    /**
     * @return Node participant map.
     */
    @Nullable @Override public Map<UUID, IgniteUuid> participants() {
        if (clsLdr instanceof GridDeploymentClassLoader)
            return ((GridDeploymentInfo)clsLdr).participants();

        return null;
    }

    /**
     * Deployment callback.
     *
     * @param cls Deployed class.
     */
    public void onDeployed(Class<?> cls) {
        // No-op.
    }

    /**
     * @param cls Class to get annotation for.
     * @param annCls Annotation class.
     * @return Annotation value.
     * @param <T> Annotation class.
     */
    @SuppressWarnings("unchecked")
    public <T extends Annotation> T annotation(Class<?> cls, Class<T> annCls) {
        ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>> clsAnns = anns.get(cls);

        if (clsAnns == null) {
            ConcurrentMap<Class<? extends Annotation>, GridTuple<Annotation>> old = anns.putIfAbsent(cls,
                clsAnns = new ConcurrentHashMap8<>());

            if (old != null)
                clsAnns = old;
        }

        GridTuple<T> ann = (GridTuple<T>)clsAnns.get(annCls);

        if (ann == null) {
            ann = F.t(U.getAnnotation(cls, annCls));

            clsAnns.putIfAbsent(annCls, (GridTuple<Annotation>)ann);
        }

        return ann.get();
    }

    /**
     * Checks whether task class is annotated with {@link GridInternal}.
     *
     * @param task Task.
     * @param taskCls Task class.
     * @return {@code True} if task is internal.
     */
    @SuppressWarnings("unchecked")
    public boolean internalTask(@Nullable ComputeTask task, Class<?> taskCls) {
        assert taskCls != null;

        Boolean res = internalTasks.get(taskCls);

        if (res == null) {
            res = annotation(task instanceof GridPeerDeployAware ?
                ((GridPeerDeployAware)task).deployClass() : taskCls,
                GridInternal.class) != null;

            internalTasks.put(taskCls, res);
        }

        return res;
    }

    /**
     * @param cls Class to create new instance of (using default constructor).
     * @return New instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <T> T newInstance(Class<T> cls) throws IgniteCheckedException {
        assert cls != null;

        GridTuple<Constructor<?>> t = dfltCtorsCache.get(cls);

        if (t == null) {
            try {
                Constructor<T> ctor = cls.getDeclaredConstructor();

                if (ctor != null && !ctor.isAccessible())
                    ctor.setAccessible(true);

                dfltCtorsCache.putIfAbsent(cls, t = F.<Constructor<?>>t(ctor));
            }
            catch (NoSuchMethodException e) {
                throw new IgniteCheckedException("Failed to find empty constructor for class: " + cls, e);
            }
        }

        Constructor<?> ctor = t.get();

        if (ctor == null)
            return null;

        try {
            return (T)ctor.newInstance();
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to create new instance for class: " + cls, e);
        }
    }

    /**
     * @param clsName Class name to check.
     * @return Class for given name if it was previously deployed.
     */
    public Class<?> existingDeployedClass(String clsName) {
        return clss.get(clsName);
    }

    /**
     * Gets class for a name.
     *
     * @param clsName Class name.
     * @param alias Optional array of aliases.
     * @return Class for given name.
     */
    @SuppressWarnings({"StringEquality", "UnusedCatchParameter"})
    @Nullable public Class<?> deployedClass(String clsName, String... alias) {
        Class<?> cls = clss.get(clsName);

        if (cls == null) {
            try {
                cls = Class.forName(clsName, true, clsLdr);

                Class<?> cur = clss.putIfAbsent(clsName, cls);

                if (cur == null) {
                    for (String a : alias) {
                        clss.putIfAbsent(a, cls);
                    }

                    onDeployed(cls);
                }
            }
            catch (ClassNotFoundException ignored) {
                // Check aliases.
                for (String a : alias) {
                    cls = clss.get(a);

                    if (cls != null)
                        return cls;
                    else if (!a.equals(clsName)) {
                        try {
                            cls = Class.forName(a, true, clsLdr);
                        }
                        catch (ClassNotFoundException ignored0) {
                            continue;
                        }

                        Class<?> cur = clss.putIfAbsent(a, cls);

                        if (cur == null) {
                            for (String a1 : alias) {
                                // The original alias has already been put into the map,
                                // so we don't try to put it again here.
                                if (a1 != a)
                                    clss.putIfAbsent(a1, cls);
                            }

                            onDeployed(cls);
                        }

                        return cls;
                    }
                }
            }
        }

        return cls;
    }

    /**
     * Adds deployed class together with aliases.
     *
     * @param cls Deployed class.
     * @param aliases Class aliases.
     * @return {@code True} if class was added.
     */
    public boolean addDeployedClass(Class<?> cls, String... aliases) {
        boolean res = false;

        if (cls != null) {
            Class<?> cur = clss.putIfAbsent(cls.getName(), cls);

            if (cur == null) {
                onDeployed(cls);

                res = true;
            }

            for (String alias : aliases) {
                if (alias != null)
                    clss.putIfAbsent(alias, cls);
            }
        }

        return res;
    }

    /**
     * @return Deployed classes.
     */
    public Collection<Class<?>> deployedClasses() {
        return Collections.unmodifiableCollection(clss.values());
    }

    /**
     * @return Deployed class map, keyed by class name or alias.
     */
    public Map<String, Class<?>> deployedClassMap() {
        return Collections.unmodifiableMap(clss);
    }

    /**
     * Gets value of annotated field or method.
     *
     * @param target Object to find a value in.
     * @param annCls Annotation class.
     * @return Value of annotated field or method.
     * @throws IgniteCheckedException If failed to find.
     */
    @Nullable public Object annotatedValue(Object target, Class<? extends Annotation> annCls) throws IgniteCheckedException {
        return annotatedValue(target, annCls, null, false).get1();
    }

    /**
     * @param target Object to find a value in.
     * @param annCls Annotation class.
     * @param visited Set of visited objects to avoid cycling.
     * @param annFound Flag indicating if value has already been found.
     * @return Value of annotated field or method.
     * @throws IgniteCheckedException If failed to find.
     */
    private IgniteBiTuple<Object, Boolean> annotatedValue(Object target, Class<? extends Annotation> annCls,
        @Nullable Set<Object> visited, boolean annFound) throws IgniteCheckedException {
        assert target != null;

        // To avoid infinite recursion.
        if (visited != null && visited.contains(target))
            return F.t(null, annFound);

        Object val = null;

        for (Class<?> cls = target.getClass(); !cls.equals(Object.class); cls = cls.getSuperclass()) {
            // Fields.
            for (Field f : fieldsWithAnnotation(cls, annCls)) {
                f.setAccessible(true);

                Object fieldVal;

                try {
                    fieldVal = f.get(target);
                }
                catch (IllegalAccessException e) {
                    throw new IgniteCheckedException("Failed to get annotated field value [cls=" + cls.getName() +
                        ", ann=" + annCls.getSimpleName(), e);
                }

                if (needsRecursion(f)) {
                    if (fieldVal != null) {
                        if (visited == null)
                            visited = new GridLeanSet<>();

                        visited.add(target);

                        // Recursion.
                        IgniteBiTuple<Object, Boolean> tup = annotatedValue(fieldVal, annCls, visited, annFound);

                        if (!annFound && tup.get2())
                            // Update value only if annotation was found in recursive call.
                            val = tup.get1();

                        annFound = tup.get2();
                    }
                }
                else {
                    if (annFound)
                        throw new IgniteCheckedException("Multiple annotations have been found [cls=" + cls.getName() +
                            ", ann=" + annCls.getSimpleName() + "]");

                    val = fieldVal;

                    annFound = true;
                }
            }

            // Methods.
            for (Method m : methodsWithAnnotation(cls, annCls)) {
                if (annFound)
                    throw new IgniteCheckedException("Multiple annotations have been found [cls=" + cls.getName() +
                        ", ann=" + annCls.getSimpleName() + "]");

                m.setAccessible(true);

                try {
                    val = m.invoke(target);
                }
                catch (Exception e) {
                    throw new IgniteCheckedException("Failed to get annotated method value [cls=" + cls.getName() +
                        ", ann=" + annCls.getSimpleName(), e);
                }

                annFound = true;
            }
        }

        return F.t(val, annFound);
    }

    /**
     * Gets all entries from the specified class or its super-classes that have
     * been annotated with annotation provided.
     *
     * @param cls Class in which search for methods.
     * @param annCls Annotation.
     * @return Set of entries with given annotations.
     */
    private Iterable<Field> fieldsWithAnnotation(Class<?> cls, Class<? extends Annotation> annCls) {
        assert cls != null;
        assert annCls != null;

        Collection<Field> fields = fieldsFromCache(cls, annCls);

        if (fields == null) {
            fields = new ArrayList<>();

            for (Field field : cls.getDeclaredFields()) {
                Annotation ann = field.getAnnotation(annCls);

                if (ann != null || needsRecursion(field))
                    fields.add(field);
            }

            cacheFields(cls, annCls, fields);
        }

        return fields;
    }

    /**
     * Gets set of methods with given annotation.
     *
     * @param cls Class in which search for methods.
     * @param annCls Annotation.
     * @return Set of methods with given annotations.
     */
    private Iterable<Method> methodsWithAnnotation(Class<?> cls, Class<? extends Annotation> annCls) {
        assert cls != null;
        assert annCls != null;

        Collection<Method> mtds = methodsFromCache(cls, annCls);

        if (mtds == null) {
            mtds = new ArrayList<>();

            for (Method mtd : cls.getDeclaredMethods()) {
                Annotation ann = mtd.getAnnotation(annCls);

                if (ann != null)
                    mtds.add(mtd);
            }

            cacheMethods(cls, annCls, mtds);
        }

        return mtds;
    }

    /**
     * @param f Field.
     * @return {@code true} if recursive inspection is required.
     */
    private boolean needsRecursion(Field f) {
        assert f != null;

        // Need to inspect anonymous classes, callable and runnable instances.
        return f.getName().startsWith("this$") || f.getName().startsWith("val$") ||
            Callable.class.isAssignableFrom(f.getType()) || Runnable.class.isAssignableFrom(f.getType());
    }

    /**
     * Gets all fields for a given class with given annotation from cache.
     *
     * @param cls Class to get fields from.
     * @param annCls Annotation class for fields.
     * @return List of fields with given annotation, possibly {@code null}.
     */
    @Nullable private Collection<Field> fieldsFromCache(Class<?> cls, Class<? extends Annotation> annCls) {
        assert cls != null;
        assert annCls != null;

        Map<Class<? extends Annotation>, Collection<Field>> annCache = fieldCache.get(cls);

        return annCache != null ? annCache.get(annCls) : null;
    }

    /**
     * Caches list of fields with given annotation from given class.
     *
     * @param cls Class the fields belong to.
     * @param annCls Annotation class for the fields.
     * @param fields Fields to cache.
     */
    private void cacheFields(Class<?> cls, Class<? extends Annotation> annCls, Collection<Field> fields) {
        assert cls != null;
        assert annCls != null;
        assert fields != null;

        Map<Class<? extends Annotation>, Collection<Field>> annFields = F.addIfAbsent(fieldCache,
            cls, F.<Class<? extends Annotation>, Collection<Field>>newCMap());

        assert annFields != null;

        annFields.put(annCls, fields);
    }

    /**
     * Gets all methods for a given class with given annotation from cache.
     *
     * @param cls Class to get methods from.
     * @param annCls Annotation class for fields.
     * @return List of methods with given annotation, possibly {@code null}.
     */
    @Nullable private Collection<Method> methodsFromCache(Class<?> cls, Class<? extends Annotation> annCls) {
        assert cls != null;
        assert annCls != null;

        Map<Class<? extends Annotation>, Collection<Method>> annCache = mtdCache.get(cls);

        return annCache != null ? annCache.get(annCls) : null;
    }

    /**
     * Caches list of methods with given annotation from given class.
     *
     * @param cls Class the fields belong to.
     * @param annCls Annotation class for the fields.
     * @param mtds Methods to cache.
     */
    private void cacheMethods(Class<?> cls, Class<? extends Annotation> annCls,
        Collection<Method> mtds) {
        assert cls != null;
        assert annCls != null;
        assert mtds != null;

        Map<Class<? extends Annotation>, Collection<Method>> annMtds = F.addIfAbsent(mtdCache,
            cls, F.<Class<? extends Annotation>, Collection<Method>>newCMap());

        assert annMtds != null;

        annMtds.put(annCls, mtds);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        int[] stamp = new int[1];

        boolean undeployed = usage.get(stamp);

        return S.toString(GridDeployment.class, this, "undeployed", undeployed, "usage", stamp[0]);
    }
}