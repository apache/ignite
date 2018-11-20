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

package org.apache.ignite.compute.gridify.aop.spring;

import java.lang.reflect.Method;
import org.apache.ignite.compute.gridify.Gridify;
import org.apache.ignite.compute.gridify.GridifySetToSet;
import org.apache.ignite.compute.gridify.GridifySetToValue;
import org.springframework.aop.ClassFilter;
import org.springframework.aop.MethodMatcher;
import org.springframework.aop.Pointcut;

/**
 * Pointcut used by gridified aspects to find methods
 * annotated with {@link org.apache.ignite.compute.gridify.Gridify}, {@link GridifySetToValue} and
 * {@link GridifySetToSet} annotations.
 */
public class GridifySpringPointcut implements Pointcut {
    /**
     * Class filter.
     */
    private static final ClassFilter filter = new ClassFilter() {
        @SuppressWarnings({"unchecked", "RawUseOfParameterizedType"})
        @Override public boolean matches(Class cls) {
            return true;
        }
    };

    /** Method matcher. */
    private static final MethodMatcher dfltMatcher = new GridifyMethodMatcher() {
        // Warning suppression is due to Spring...
        @SuppressWarnings("unchecked")
        @Override public boolean matches(Method method, Class cls) {
            return cls.isAnnotationPresent(Gridify.class) || method.isAnnotationPresent(Gridify.class);
        }
    };

    /** Method matcher. */
    private static final MethodMatcher setToValueMatcher = new GridifyMethodMatcher() {
        // Warning suppression is due to Spring...
        @SuppressWarnings("unchecked")
        @Override public boolean matches(Method method, Class cls) {
            return cls.isAnnotationPresent(GridifySetToValue.class) || method.isAnnotationPresent(GridifySetToValue.class);
        }
    };

    /** Method matcher. */
    private static final MethodMatcher setToSetMatcher = new GridifyMethodMatcher() {
        // Warning suppression is due to Spring...
        @SuppressWarnings("unchecked")
        @Override public boolean matches(Method method, Class cls) {
            return cls.isAnnotationPresent(GridifySetToSet.class) || method.isAnnotationPresent(GridifySetToSet.class);
        }
    };

    /** */
    private final GridifySpringPointcutType type;

    /**
     * Creates pointcut associated with specific aspect.
     *
     * @param type Type.
     */
    public GridifySpringPointcut(GridifySpringPointcutType type) {
        assert type != null;

        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public ClassFilter getClassFilter() {
        return filter;
    }

    /** {@inheritDoc} */
    @Override public MethodMatcher getMethodMatcher() {
        switch (type) {
            case DFLT: return dfltMatcher;
            case SET_TO_VALUE: return setToValueMatcher;
            case SET_TO_SET: return setToSetMatcher;

            default:
                assert false : "Unknown pointcut type: " + type;
        }

        return null;
    }

    /**
     * Method matcher.
     */
    private abstract static class GridifyMethodMatcher implements MethodMatcher {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public abstract boolean matches(Method method, Class cls);

        /** {@inheritDoc} */
        @Override public boolean isRuntime() {
            return false;
        }

        /** {@inheritDoc} */
        // Warning suppression is due to Spring...
        @SuppressWarnings({"unchecked", "RawUseOfParameterizedType"})
        @Override public boolean matches(Method method, Class aClass, Object[] objs) {
            // No-op.
            return false;
        }
    }

    /**
     * Pointcut type.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public enum GridifySpringPointcutType {
        /** */
        DFLT,

        /** */
        SET_TO_VALUE,

        /** */
        SET_TO_SET
    }
}