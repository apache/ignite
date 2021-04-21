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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.util.GridLeanIdentitySet;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoadBalancerResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.resources.TaskContinuousMapperResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.jetbrains.annotations.Nullable;

/**
 * Resource container contains caches for classes used for injection.
 * Caches used to improve the efficiency of standard Java reflection mechanism.
 */
public class GridResourceIoc {
    /** Task class resource mapping. Used to efficiently cleanup resources related to class loader. */
    private final ConcurrentMap<ClassLoader, Set<Class<?>>> taskMap = new ConcurrentHashMap<>();

    /** Class descriptors cache. */
    private AtomicReference<Map<Class<?>, ClassDescriptor>> clsDescs = new AtomicReference<>();

    /**
     * @param ldr Class loader.
     */
    void onUndeployed(ClassLoader ldr) {
        Set<Class<?>> clss = taskMap.remove(ldr);

        if (clss != null) {
            Map<Class<?>, ClassDescriptor> newMap, oldMap;

            do {
                oldMap = clsDescs.get();

                if (oldMap == null)
                    break;

                newMap = new HashMap<>(oldMap.size() - clss.size());

                for (Map.Entry<Class<?>, ClassDescriptor> entry : oldMap.entrySet()) {
                    if (!clss.contains(entry.getKey()))
                        newMap.put(entry.getKey(), entry.getValue());
                }
            }
            while (!clsDescs.compareAndSet(oldMap, newMap));
        }
    }

    /**
     * Clears all internal caches.
     */
    void undeployAll() {
        taskMap.clear();

        clsDescs.set(null);
    }

    /**
     * Injects given resource via field or setter with specified annotations on provided target object.
     *
     * @param target Target object.
     * @param annCls Setter annotation.
     * @param injector Resource to inject.
     * @param dep Deployment.
     * @param depCls Deployment class.
     * @return {@code True} if resource was injected.
     * @throws IgniteCheckedException Thrown in case of any errors during injection.
     */
    boolean inject(Object target,
        Class<? extends Annotation> annCls,
        GridResourceInjector injector,
        @Nullable GridDeployment dep,
        @Nullable Class<?> depCls)
        throws IgniteCheckedException {
        return injectInternal(target, annCls, injector, dep, depCls, null);
    }

    /**
     * @param dep Deployment.
     * @param cls Class.
     * @return Descriptor.
     */
    ClassDescriptor descriptor(@Nullable GridDeployment dep, Class<?> cls) {
        Map<Class<?>, ClassDescriptor> newMap, oldMap;
        ClassDescriptor res, newDesc = null;

        do {
            oldMap = clsDescs.get();

            if (oldMap != null && (res = oldMap.get(cls)) != null)
                break;

            if (dep != null) {
                Set<Class<?>> classes = F.addIfAbsent(taskMap, dep.classLoader(), F.newCSet());

                classes.add(cls);

                dep = null;
            }

            if (oldMap == null)
                newMap = new HashMap<>();
            else
                (newMap = new HashMap<>(oldMap.size() + 1)).putAll(oldMap);

            newMap.put(cls, res = newDesc == null ? (newDesc = new ClassDescriptor(cls)) : newDesc);
        }
        while (!clsDescs.compareAndSet(oldMap, newMap));

        return res;
    }

    /**
     * @param target Target object.
     * @param annCls Setter annotation.
     * @param injector Resource to inject.
     * @param dep Deployment.
     * @param depCls Deployment class.
     * @param checkedObjs Set of already inspected objects to avoid indefinite recursion.
     * @return {@code True} if resource was injected.
     * @throws IgniteCheckedException Thrown in case of any errors during injection.
     */
    private boolean injectInternal(Object target,
        Class<? extends Annotation> annCls,
        GridResourceInjector injector,
        @Nullable GridDeployment dep,
        @Nullable Class<?> depCls,
        @Nullable Set<Object> checkedObjs)
        throws IgniteCheckedException {
        Class<?> targetCls = target.getClass();

        ClassDescriptor descr = descriptor(dep, targetCls);

        T2<GridResourceField[], GridResourceMethod[]> annotatedMembers = descr.annotatedMembers(annCls);

        return descr.injectInternal(target, annCls, annotatedMembers, injector, dep, depCls, checkedObjs);
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
     * Checks if annotation is presented on a field or method of the specified object.
     *
     * @param target Target object.
     * @param annSet Annotation classes to find on fields or methods of target object.
     * @param dep Deployment.
     * @return {@code true} if any annotation is presented, {@code false} if it's not.
     */
    boolean isAnnotationsPresent(@Nullable GridDeployment dep, Object target, AnnotationSet annSet) {
        assert target != null;
        assert annSet != null;

        return descriptor(dep, target.getClass()).isAnnotated(annSet) != 0;
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

    /** Print memory statistics */
    public void printMemoryStats() {
        X.println(">>>   taskMapSize: " + taskMap.size());

        Map<Class<?>, ClassDescriptor> map = clsDescs.get();
        X.println(">>>   classDescriptorsCacheSize: " + (map == null ? 0 : map.size()));
    }

    /**
     *
     */
    class ClassDescriptor {
        /** */
        private final Field[] recursiveFields;

        /** */
        private final Map<Class<? extends Annotation>, T2<GridResourceField[], GridResourceMethod[]>> annMap;

        /**
         * Uses as enum-map with enum {@link AnnotationSet} member as key,
         * and bitmap as a result of matching found annotations with enum set {@link ResourceAnnotation} as value.
         */
        private final int[] containsAnnSets;

        /** Uses as enum-map with enum {@link ResourceAnnotation} member as a keys. */
        private final T2<GridResourceField[], GridResourceMethod[]>[] annArr;

        /**
         * @param cls Class.
         */
        @SuppressWarnings("unchecked")
        ClassDescriptor(Class<?> cls) {
            Map<Class<? extends Annotation>, T2<List<GridResourceField>, List<GridResourceMethod>>> annMap
                = new HashMap<>();

            List<Field> recursiveFieldsList = new ArrayList<>();

            boolean allowImplicitInjection = !GridNoImplicitInjection.class.isAssignableFrom(cls);

            for (Class<?> cls0 = cls; !cls0.equals(Object.class); cls0 = cls0.getSuperclass()) {
                for (Field field : cls0.getDeclaredFields()) {
                    Annotation[] fieldAnns = field.getAnnotations();

                    for (Annotation ann : fieldAnns) {
                        T2<List<GridResourceField>, List<GridResourceMethod>> t2 = annMap.get(ann.annotationType());

                        if (t2 == null) {
                            t2 = new T2<>(new ArrayList<>(), new ArrayList<>());

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
                            t2 = new T2<>(new ArrayList<>(), new ArrayList<>());

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

            T2<GridResourceField[], GridResourceMethod[]>[] annArr = null;

            if (annMap.isEmpty())
                containsAnnSets = null;
            else {
                int annotationsBits = 0;

                for (ResourceAnnotation ann : ResourceAnnotation.values()) {
                    T2<GridResourceField[], GridResourceMethod[]> member = annotatedMembers(ann.clazz);

                    if (member != null) {
                        if (annArr == null)
                            annArr = new T2[ResourceAnnotation.values().length];

                        annArr[ann.ordinal()] = member;

                        annotationsBits |= 1 << ann.ordinal();
                    }
                }

                AnnotationSet[] annotationSets = AnnotationSet.values();

                containsAnnSets = new int[annotationSets.length];

                for (int i = 0; i < annotationSets.length; i++)
                    containsAnnSets[i] = annotationsBits & annotationSets[i].annotationsBitSet;
            }

            this.annArr = annArr;
        }

        /**
         * @return Recursive fields.
         */
        Field[] recursiveFields() {
            return recursiveFields;
        }

        /**
         * @param annCls Annotation class.
         * @return Fields.
         */
        @Nullable T2<GridResourceField[], GridResourceMethod[]> annotatedMembers(Class<? extends Annotation> annCls) {
            return annMap.get(annCls);
        }

        /**
         * @param set annotation set.
         * @return {@code Bitmask} > 0 if any annotation is presented, otherwise return 0;
         */
        int isAnnotated(AnnotationSet set) {
            return recursiveFields.length > 0 ? set.annotationsBitSet :
                (containsAnnSets == null ? 0 : containsAnnSets[set.ordinal()]);
        }

        /**
         * @param ann Annotation.
         * @return {@code True} if annotation is presented.
         */
        boolean isAnnotated(ResourceAnnotation ann) {
            return recursiveFields.length > 0 || (annArr != null && annArr[ann.ordinal()] != null);
        }

        /**
         * @param target Target object.
         * @param annCls Annotation class.
         * @param annotatedMembers Setter annotation.
         * @param injector Resource to inject.
         * @param dep Deployment.
         * @param depCls Deployment class.
         * @param checkedObjs Set of already inspected objects to avoid indefinite recursion.
         * @return {@code True} if resource was injected.
         * @throws IgniteCheckedException Thrown in case of any errors during injection.
         */
        boolean injectInternal(Object target,
            Class<? extends Annotation> annCls,
            T2<GridResourceField[], GridResourceMethod[]> annotatedMembers,
            GridResourceInjector injector,
            @Nullable GridDeployment dep,
            @Nullable Class<?> depCls,
            @Nullable Set<Object> checkedObjs)
            throws IgniteCheckedException {
            if (recursiveFields.length == 0 && annotatedMembers == null)
                return false;

            if (checkedObjs == null && recursiveFields.length > 0)
                checkedObjs = new GridLeanIdentitySet<>();

            if (checkedObjs != null && !checkedObjs.add(target))
                return false;

            boolean injected = false;

            for (Field field : recursiveFields) {
                try {
                    Object obj = field.get(target);

                    if (obj != null) {
                        assert checkedObjs != null;

                        ClassDescriptor desc = descriptor(dep, obj.getClass());
                        injected |= desc.injectInternal(obj, annCls, desc.annotatedMembers(annCls),
                            injector, dep, depCls, checkedObjs);
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
         * @param target Target object.
         * @param ann Setter annotation.
         * @param injector Resource to inject.
         * @param dep Deployment.
         * @param depCls Deployment class.
         * @return {@code True} if resource was injected.
         * @throws IgniteCheckedException Thrown in case of any errors during injection.
         */
        public boolean inject(Object target,
            ResourceAnnotation ann,
            GridResourceInjector injector,
            @Nullable GridDeployment dep,
            @Nullable Class<?> depCls)
            throws IgniteCheckedException {
            return injectInternal(target,
                ann.clazz,
                annArr == null ? null : annArr[ann.ordinal()],
                injector,
                dep,
                depCls,
                null);
        }
    }

    /**
     *
     */
    enum ResourceAnnotation {
        /** */
        CACHE_NAME(CacheNameResource.class),

        /** */
        SPRING_APPLICATION_CONTEXT(SpringApplicationContextResource.class),

        /** */
        SPRING(SpringResource.class),

        /** */
        IGNITE_INSTANCE(IgniteInstanceResource.class),

        /** */
        LOGGER(LoggerResource.class),

        /** */
        SERVICE(ServiceResource.class),

        /** */
        TASK_SESSION(TaskSessionResource.class),

        /** */
        LOAD_BALANCER(LoadBalancerResource.class),

        /** */
        TASK_CONTINUOUS_MAPPER(TaskContinuousMapperResource.class),

        /** */
        JOB_CONTEXT(JobContextResource.class),

        /** */
        CACHE_STORE_SESSION(CacheStoreSessionResource.class);

        /** */
        public final Class<? extends Annotation> clazz;

        /**
         * @param clazz annotation class.
         */
        ResourceAnnotation(Class<? extends Annotation> clazz) {
            this.clazz = clazz;
        }
    }

    /**
     *
     */
    public enum AnnotationSet {
        /** */
        GENERIC(
            ResourceAnnotation.SPRING_APPLICATION_CONTEXT,
            ResourceAnnotation.SPRING,
            ResourceAnnotation.IGNITE_INSTANCE,
            ResourceAnnotation.LOGGER,
            ResourceAnnotation.SERVICE
        ),

        /** */
        ENTRY_PROCESSOR(
            ResourceAnnotation.CACHE_NAME,

            ResourceAnnotation.SPRING_APPLICATION_CONTEXT,
            ResourceAnnotation.SPRING,
            ResourceAnnotation.IGNITE_INSTANCE,
            ResourceAnnotation.LOGGER,
            ResourceAnnotation.SERVICE
        ),

        /** */
        TASK(
            ResourceAnnotation.TASK_SESSION,
            ResourceAnnotation.LOAD_BALANCER,
            ResourceAnnotation.TASK_CONTINUOUS_MAPPER,

            ResourceAnnotation.SPRING_APPLICATION_CONTEXT,
            ResourceAnnotation.SPRING,
            ResourceAnnotation.IGNITE_INSTANCE,
            ResourceAnnotation.LOGGER,
            ResourceAnnotation.SERVICE
        ),

        /** */
        JOB(
            ResourceAnnotation.TASK_SESSION,
            ResourceAnnotation.JOB_CONTEXT,

            ResourceAnnotation.SPRING_APPLICATION_CONTEXT,
            ResourceAnnotation.SPRING,
            ResourceAnnotation.IGNITE_INSTANCE,
            ResourceAnnotation.LOGGER,
            ResourceAnnotation.SERVICE
        );

        /** Resource annotations bits for fast checks. */
        public final int annotationsBitSet;

        /** Holds annotations in order */
        public final ResourceAnnotation[] annotations;

        /**
         * @param annotations ResourceAnnotations.
         */
        AnnotationSet(ResourceAnnotation... annotations) {
            assert annotations.length < 32 : annotations.length;

            this.annotations = annotations;

            int mask = 0;

            for (ResourceAnnotation ann : annotations)
                mask |= 1 << ann.ordinal();

            annotationsBitSet = mask;
        }
    }
}
