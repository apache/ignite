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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.util.GridLeanIdentitySet;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * Resource container contains caches for classes used for injection.
 * Caches used to improve the efficiency of standard Java reflection mechanism.
 */
class GridResourceIoc {
    /** Task class resource mapping. Used to efficiently cleanup resources related to class loader. */
    private final ConcurrentMap<ClassLoader, Set<Class<?>>> taskMap =
        new ConcurrentHashMap8<>();

    /** Class descriptors cache. */
    private final ConcurrentMap<Class<?>, ClassDescriptor> clsDescs = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<Class<?>, Class<? extends Annotation>[]> annCache =
        new ConcurrentHashMap8<>();

    /**
     * @param ldr Class loader.
     */
    void onUndeployed(ClassLoader ldr) {
        Set<Class<?>> clss = taskMap.remove(ldr);

        if (clss != null) {
            clsDescs.keySet().removeAll(clss);
            annCache.keySet().removeAll(clss);
        }
    }

    /**
     * Clears all internal caches.
     */
    void undeployAll() {
        taskMap.clear();
        clsDescs.clear();
        annCache.clear();
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
        return injectInternal(target, annCls, injector, dep, depCls, null);
    }

    /**
     * @param cls Class.
     */
    private ClassDescriptor descriptor(@Nullable GridDeployment dep, Class<?> cls) {
        ClassDescriptor res = clsDescs.get(cls);

        if (res == null) {
            if (dep != null) {
                Set<Class<?>> classes = F.addIfAbsent(taskMap, dep.classLoader(), F.<Class<?>>newCSet());

                classes.add(cls);
            }

            res = F.addIfAbsent(clsDescs, cls, new ClassDescriptor(cls));
        }

        return res;
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
        @Nullable Set<Object> checkedObjs)
        throws IgniteCheckedException
    {
        Class<?> targetCls = target.getClass();

        ClassDescriptor descr = descriptor(dep, targetCls);

        T2<GridResourceField[], GridResourceMethod[]> annotatedMembers = descr.annotatedMembers(annCls);

        if (descr.recursiveFields().length == 0 && annotatedMembers == null)
            return false;

        if (checkedObjs == null && descr.recursiveFields().length > 0)
            checkedObjs = new GridLeanIdentitySet<>();

        if (checkedObjs != null && !checkedObjs.add(target))
            return false;

        boolean injected = false;

        for (Field field : descr.recursiveFields()) {
            try {
                Object obj = field.get(target);

                if (obj != null) {
                    assert checkedObjs != null;

                    injected |= injectInternal(obj, annCls, injector, dep, depCls, checkedObjs);
                }
            }
            catch (IllegalAccessException e) {
                throw new IgniteCheckedException("Failed to inject resource [field=" + field.getName() +
                    ", target=" + target + ']', e);
            }
        }

        if (annotatedMembers != null) {
            for (GridResourceField field : annotatedMembers.get1()) {
                injector.inject(field, target, depCls, dep);

                injected = true;
            }

            for (GridResourceMethod mtd : annotatedMembers.get2()) {
                injector.inject(mtd, target, depCls, dep);

                injected = true;
            }
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

        ClassDescriptor desc = descriptor(dep, target.getClass());

        return desc.recursiveFields().length > 0 || desc.annotatedMembers(annCls) != null;
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
            Collection<Class<? extends Annotation>> res0 = new ArrayList<>();

            for (Class<? extends Annotation> annCls : annClss) {
                if (isAnnotationPresent(target, annCls, dep))
                    res0.add(annCls);
            }

            res = res0.toArray(new Class[res0.size()]);

            annCache.putIfAbsent(cls, res);
        }

        return res;
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
        ClassDescriptor desc = descriptor(dep, cls);

        T2<GridResourceField[], GridResourceMethod[]> t2 = desc.annotatedMembers(annCls);

        return t2 == null ? GridResourceMethod.EMPTY_ARRAY : t2.get2();
    }

    /** {@inheritDoc} */
    public void printMemoryStats() {
        X.println(">>>   taskMapSize: " + taskMap.size());
        X.println(">>>   classDescriptorsCacheSize: " + clsDescs.size());
    }

    /**
     *
     */
    private static class ClassDescriptor {
        /** */
        private final Field[] recursiveFields;

        /** */
        private final Map<Class<? extends Annotation>, T2<GridResourceField[], GridResourceMethod[]>> annMap;

        /**
         * @param cls Class.
         */
        ClassDescriptor(Class<?> cls) {
            Map<Class<? extends Annotation>, T2<List<GridResourceField>, List<GridResourceMethod>>> annMap
                = new HashMap<>();

            List<Field> recursiveFieldsList = new ArrayList<>();

            boolean allowImplicitInjection = !GridNoImplicitInjection.class.isAssignableFrom(cls);

            for (Class cls0 = cls; !cls0.equals(Object.class); cls0 = cls0.getSuperclass()) {
                for (Field field : cls0.getDeclaredFields()) {
                    Annotation[] fieldAnns = field.getAnnotations();

                    for (Annotation ann : fieldAnns) {
                        T2<List<GridResourceField>, List<GridResourceMethod>> t2 = annMap.get(ann.annotationType());

                        if (t2 == null) {
                            t2 = new T2<List<GridResourceField>, List<GridResourceMethod>>(
                                new ArrayList<GridResourceField>(),
                                new ArrayList<GridResourceMethod>());

                            annMap.put(ann.annotationType(), t2);
                        }

                        t2.get1().add(new GridResourceField(field, ann));
                    }

                    if (allowImplicitInjection
                        && fieldAnns.length == 0
                        && GridResourceUtils.mayRequireResources(field)) {
                        field.setAccessible(true);

                        // Account for anonymous inner classes.
                        recursiveFieldsList.add(field);
                    }
                }

                for (Method mtd : cls0.getDeclaredMethods()) {
                    for (Annotation ann : mtd.getAnnotations()) {
                        T2<List<GridResourceField>, List<GridResourceMethod>> t2 = annMap.get(ann.annotationType());

                        if (t2 == null) {
                            t2 = new T2<List<GridResourceField>, List<GridResourceMethod>>(
                                new ArrayList<GridResourceField>(),
                                new ArrayList<GridResourceMethod>());

                            annMap.put(ann.annotationType(), t2);
                        }

                        t2.get2().add(new GridResourceMethod(mtd, ann));
                    }
                }
            }

            recursiveFields = recursiveFieldsList.isEmpty() ? U.EMPTY_FIELDS
                : recursiveFieldsList.toArray(new Field[recursiveFieldsList.size()]);

            this.annMap = IgniteUtils.limitedMap(annMap.size());

            for (Map.Entry<Class<? extends Annotation>, T2<List<GridResourceField>, List<GridResourceMethod>>> entry
                : annMap.entrySet()) {
                GridResourceField[] fields = GridResourceField.toArray(entry.getValue().get1());
                GridResourceMethod[] mtds = GridResourceMethod.toArray(entry.getValue().get2());

                this.annMap.put(entry.getKey(), new T2<>(fields, mtds));
            }
        }

        /**
         * @return Recursive fields.
         */
        public Field[] recursiveFields() {
            return recursiveFields;
        }

        /**
         * @return Fields.
         */
        @Nullable public T2<GridResourceField[], GridResourceMethod[]> annotatedMembers(Class<? extends Annotation> annCls) {
            return annMap.get(annCls);
        }
    }
}