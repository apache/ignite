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

package org.apache.ignite.internal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.lang.annotation.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Resource container contains caches for classes used for injection.
 * Caches used to improve the efficiency of standard Java reflection mechanism.
 */
class GridResourceIoc {
    /** Task class resource mapping. Used to efficiently cleanup resources related to class loader. */
    private final ConcurrentMap<ClassLoader, Set<Class<?>>> taskMap =
        new ConcurrentHashMap8<>();

    /** Field cache. */
    private final ConcurrentMap<Class<?>, ConcurrentMap<Class<? extends Annotation>, GridResourceField[]>> fieldCache =
        new ConcurrentHashMap8<>();

    /** Method cache. */
    private final ConcurrentMap<Class<?>, ConcurrentMap<Class<? extends Annotation>, GridResourceMethod[]>> mtdCache =
        new ConcurrentHashMap8<>();

    /**
     * Cache for classes that do not require injection with some annotation.
     * Maps annotation classes to set a set of target classes to skip.
     */
    private final ConcurrentMap<Class<? extends Annotation>, Set<Class<?>>> skipCache =
        new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<Class<?>, Class<? extends Annotation>[]> annCache =
        new ConcurrentHashMap8<>();

    /**
     * @param ldr Class loader.
     */
    void onUndeployed(ClassLoader ldr) {
        Set<Class<?>> clss = taskMap.remove(ldr);

        if (clss != null) {
            fieldCache.keySet().removeAll(clss);
            mtdCache.keySet().removeAll(clss);

            for (Map.Entry<Class<? extends Annotation>, Set<Class<?>>> e : skipCache.entrySet()) {
                Set<Class<?>> skipClss = e.getValue();

                if (skipClss != null)
                    e.getValue().removeAll(clss);
            }

            for (Class<?> cls : clss)
                annCache.remove(cls);
        }
    }

    /**
     * Clears all internal caches.
     */
    void undeployAll() {
        taskMap.clear();
        mtdCache.clear();
        fieldCache.clear();
    }

    /**
     * Injects given resource via field or setter with specified annotations on provided target object.
     *
     * @param target Target object.
     * @param annCls Setter annotation.
     * @param injector Resource to inject.
     * @param dep Deployment.
     * @param depCls Deployment class.
     * @throws IgniteCheckedException Thrown in case of any errors during injection.
     * @return {@code True} if resource was injected.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    boolean inject(Object target,
        Class<? extends Annotation> annCls,
        GridResourceInjector injector,
        @Nullable GridDeployment dep,
        @Nullable Class<?> depCls)
        throws IgniteCheckedException
    {
        assert target != null;
        assert annCls != null;
        assert injector != null;

        if (isAnnotationPresent(target, annCls, dep))
            // Use identity hash set to compare via referential equality.
            return injectInternal(target, annCls, injector, dep, depCls, new GridLeanIdentitySet<>());

        return false;
    }

    /**
     * @param target Target object.
     * @param annCls Setter annotation.
     * @param injector Resource to inject.
     * @param dep Deployment.
     * @param depCls Deployment class.
     * @param checkedObjs Set of already inspected objects to avoid indefinite recursion.
     * @throws IgniteCheckedException Thrown in case of any errors during injection.
     * @return {@code True} if resource was injected.
     */
    private boolean injectInternal(Object target,
        Class<? extends Annotation> annCls,
        GridResourceInjector injector,
        @Nullable GridDeployment dep,
        @Nullable Class<?> depCls,
        Set<Object> checkedObjs)
        throws IgniteCheckedException
    {
        assert target != null;
        assert annCls != null;
        assert injector != null;
        assert checkedObjs != null;

        Class<?> targetCls = target.getClass();

        Set<Class<?>> skipClss = skipCache.get(annCls);

        // Skip this class if it does not need to be injected.
        if (skipClss != null && skipClss.contains(targetCls))
            return false;

        // Check if already inspected to avoid indefinite recursion.
        if (!checkedObjs.add(target))
            return false;

        int annCnt = 0;

        boolean injected = false;

        for (GridResourceField field : getFieldsWithAnnotation(dep, targetCls, annCls)) {
            if (field.processFieldValue()) {
                Field f = field.getField();

                try {
                    Object obj = f.get(target);

                    if (obj != null) {
                        // Recursion.
                        boolean injected0 = injectInternal(obj, annCls, injector, dep, depCls, checkedObjs);

                        injected |= injected0;
                    }
                }
                catch (IllegalAccessException e) {
                    throw new IgniteCheckedException("Failed to inject resource [field=" + f.getName() +
                        ", target=" + target + ']', e);
                }
            }
            else {
                injector.inject(field, target, depCls, dep);

                injected = true;
            }

            annCnt++;
        }

        for (GridResourceMethod mtd : getMethodsWithAnnotation(dep, targetCls, annCls)) {
            injector.inject(mtd, target, depCls, dep);

            injected = true;

            annCnt++;
        }

        if (annCnt == 0) {
            if (skipClss == null)
                skipClss = F.addIfAbsent(skipCache, annCls, F.<Class<?>>newCSet());

            assert skipClss != null;

            skipClss.add(targetCls);
        }

        return injected;
    }

    /**
     * Checks if annotation is presented on a field or method of the specified object.
     *
     * @param target Target object.
     * @param annCls Annotation class to find on fields or methods of target object.
     * @param dep Deployment.
     * @return {@code true} if annotation is presented, {@code false} if it's not.
     */
    boolean isAnnotationPresent(Object target, Class<? extends Annotation> annCls, @Nullable GridDeployment dep) {
        assert target != null;
        assert annCls != null;

        Class<?> targetCls = target.getClass();

        Set<Class<?>> skipClss = skipCache.get(annCls);

        if (skipClss != null && skipClss.contains(targetCls))
            return false;

        GridResourceField[] fields = getFieldsWithAnnotation(dep, targetCls, annCls);

        if (fields.length > 0)
            return true;

        GridResourceMethod[] mtds = getMethodsWithAnnotation(dep, targetCls, annCls);

        if (mtds.length > 0)
            return true;

        if (skipClss == null)
            skipClss = F.addIfAbsent(skipCache, annCls, F.<Class<?>>newCSet());

        skipClss.add(targetCls);

        return false;
    }

    /**
     * @param dep Deployment.
     * @param target Target.
     * @param annClss Annotations.
     * @return Filtered set of annotations that present in target.
     */
    @SuppressWarnings({"SuspiciousToArrayCall", "unchecked"})
    Class<? extends Annotation>[] filter(
        @Nullable GridDeployment dep, Object target,
        Collection<Class<? extends Annotation>> annClss) {
        assert target != null;
        assert annClss != null && !annClss.isEmpty();

        Class<?> cls = target.getClass();

        Class<? extends Annotation>[] res = annCache.get(cls);

        if (res == null) {
            Collection<Class<? extends Annotation>> res0 =
                new HashSet<>(annClss.size(), 1.0f);

            for (Class<? extends Annotation> annCls : annClss) {
                if (isAnnotationPresent(target, annCls, dep))
                    res0.add(annCls);
            }

            res = new Class[res0.size()];

            res0.toArray(res);

            annCache.putIfAbsent(cls, res);
        }

        return res;
    }

    /**
     * For tests only.
     *
     * @param cls Class for test.
     * @return {@code true} if cached, {@code false} otherwise.
     */
    boolean isCached(Class<?> cls) {
        return isCached(cls.getName());
    }

    /**
     * For tests only.
     *
     * @param clsName Class for test.
     * @return {@code true} if cached, {@code false} otherwise.
     */
    boolean isCached(String clsName) {
        for (Class<?> aClass : fieldCache.keySet()) {
            if (aClass.getName().equals(clsName))
                return true;
        }

        for (Class<?> aClass : mtdCache.keySet()) {
            if (aClass.getName().equals(clsName))
                return true;
        }

        return false;
    }

    /**
     * Gets set of methods with given annotation.
     *
     * @param dep Deployment.
     * @param cls Class in which search for methods.
     * @param annCls Annotation.
     * @return Set of methods with given annotations.
     */
    GridResourceMethod[] getMethodsWithAnnotation(@Nullable GridDeployment dep, Class<?> cls,
        Class<? extends Annotation> annCls) {
        GridResourceMethod[] mtds = getMethodsFromCache(cls, annCls);

        if (mtds == null) {
            List<GridResourceMethod> mtdsList = new ArrayList<>();

            for (Class cls0 = cls; !cls0.equals(Object.class); cls0 = cls0.getSuperclass()) {
                for (Method mtd : cls0.getDeclaredMethods()) {
                    Annotation ann = mtd.getAnnotation(annCls);

                    if (ann != null)
                        mtdsList.add(new GridResourceMethod(mtd, ann));
                }
            }

            if (mtdsList.isEmpty())
                mtds = GridResourceMethod.EMPTY_ARRAY;
            else
                mtds = mtdsList.toArray(new GridResourceMethod[mtdsList.size()]);

            cacheMethods(dep, cls, annCls, mtds);
        }

        return mtds;
    }

    /**
     * Gets all entries from the specified class or its super-classes that have
     * been annotated with annotation provided.
     *
     * @param cls Class in which search for methods.
     * @param dep Deployment.
     * @param annCls Annotation.
     * @return Set of entries with given annotations.
     */
    private GridResourceField[] getFieldsWithAnnotation(@Nullable GridDeployment dep, Class<?> cls,
        Class<? extends Annotation> annCls) {
        GridResourceField[] fields = getFieldsFromCache(cls, annCls);

        if (fields == null) {
            List<GridResourceField> fieldsList = new ArrayList<>();

            boolean allowImplicitInjection = !GridNoImplicitInjection.class.isAssignableFrom(cls);

            for (Class cls0 = cls; !cls0.equals(Object.class); cls0 = cls0.getSuperclass()) {
                for (Field field : cls0.getDeclaredFields()) {
                    Annotation ann = field.getAnnotation(annCls);

                    if (ann != null)
                        fieldsList.add(new GridResourceField(field, ann));
                    else if (allowImplicitInjection && GridResourceUtils.mayRequireResources(field)) {
                        // Account for anonymous inner classes.
                        fieldsList.add(new GridResourceField(field, null));
                    }
                }
            }

            if (fieldsList.isEmpty())
                fields = GridResourceField.EMPTY_ARRAY;
            else
                fields = fieldsList.toArray(new GridResourceField[fieldsList.size()]);

            cacheFields(dep, cls, annCls, fields);
        }

        return fields;
    }

    /**
     * Gets all fields for a given class with given annotation from cache.
     *
     * @param cls Class to get fields from.
     * @param annCls Annotation class for fields.
     * @return List of fields with given annotation, possibly {@code null}.
     */
    @Nullable private GridResourceField[] getFieldsFromCache(Class<?> cls, Class<? extends Annotation> annCls) {
        Map<Class<? extends Annotation>, GridResourceField[]> annCache = fieldCache.get(cls);

        return annCache != null ? annCache.get(annCls) : null;
    }

    /**
     * Caches list of fields with given annotation from given class.
     *
     * @param cls Class the fields belong to.
     * @param dep Deployment.
     * @param annCls Annotation class for the fields.
     * @param fields Fields to cache.
     */
    private void cacheFields(@Nullable GridDeployment dep, Class<?> cls, Class<? extends Annotation> annCls,
        GridResourceField[] fields) {
        if (dep != null) {
            Set<Class<?>> classes = F.addIfAbsent(taskMap, dep.classLoader(), F.<Class<?>>newCSet());

            assert classes != null;

            classes.add(cls);
        }

        Map<Class<? extends Annotation>, GridResourceField[]> rsrcFields =
            F.addIfAbsent(fieldCache, cls, F.<Class<? extends Annotation>, GridResourceField[]>newCMap());

        assert rsrcFields != null;

        rsrcFields.put(annCls, fields);
    }

    /**
     * Gets all methods for a given class with given annotation from cache.
     *
     * @param cls Class to get methods from.
     * @param annCls Annotation class for fields.
     * @return List of methods with given annotation, possibly {@code null}.
     */
    @Nullable private GridResourceMethod[] getMethodsFromCache(Class<?> cls, Class<? extends Annotation> annCls) {
        Map<Class<? extends Annotation>, GridResourceMethod[]> annCache = mtdCache.get(cls);

        return annCache != null ? annCache.get(annCls) : null;
    }

    /**
     * Caches list of methods with given annotation from given class.
     *
     * @param rsrcCls Class the fields belong to.
     * @param dep Deployment.
     * @param annCls Annotation class for the fields.
     * @param mtds Methods to cache.
     */
    private void cacheMethods(@Nullable GridDeployment dep, Class<?> rsrcCls, Class<? extends Annotation> annCls,
        GridResourceMethod[] mtds) {
        if (dep != null) {
            Set<Class<?>> classes = F.addIfAbsent(taskMap, dep.classLoader(), F.<Class<?>>newCSet());

            assert classes != null;

            classes.add(rsrcCls);
        }

        Map<Class<? extends Annotation>, GridResourceMethod[]> rsrcMtds = F.addIfAbsent(mtdCache,
            rsrcCls, F.<Class<? extends Annotation>, GridResourceMethod[]>newCMap());

        assert rsrcMtds != null;

        rsrcMtds.put(annCls, mtds);
    }

    /** {@inheritDoc} */
    public void printMemoryStats() {
        X.println(">>>   taskMapSize: " + taskMap.size());
        X.println(">>>   fieldCacheSize: " + fieldCache.size());
        X.println(">>>   mtdCacheSize: " + mtdCache.size());
        X.println(">>>   skipCacheSize: " + skipCache.size());
    }
}
