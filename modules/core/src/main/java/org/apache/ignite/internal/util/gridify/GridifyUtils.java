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

package org.apache.ignite.internal.util.gridify;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import org.apache.ignite.compute.gridify.GridifyInput;
import org.apache.ignite.compute.gridify.GridifySetToSet;
import org.apache.ignite.compute.gridify.GridifySetToValue;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class with common methods used in gridify annotations.
 */
public final class GridifyUtils {
    /** */
    public static final int UNKNOWN_SIZE = -1;

    /** Allowed method types for return type in method. */
    private static final Class<?>[] ALLOWED_MTD_RETURN_TYPES = new Class<?>[] {
        Iterable.class,
        Iterator.class,
        Enumeration.class,
        Collection.class,
        Set.class,
        List.class,
        Queue.class,
        CharSequence.class
    };

    /** Allowed method types for input param in method for split. */
    private static final Class<?>[] ALLOWED_MTD_PARAM_TYPES = new Class<?>[] {
        Iterable.class,
        Iterator.class,
        Enumeration.class,
        Collection.class,
        Set.class,
        List.class,
        Queue.class,
        CharSequence.class
    };

    /**
     * Enforces singleton.
     */
    private GridifyUtils() {
        // No-op.
    }

    /**
     * Gets length of elements in container object with unknown type.
     * There is no ability to get size of elements for some object types
     * and method returns {@link #UNKNOWN_SIZE} value.
     *
     * @param obj Container object with elements.
     * @return Elements size of {@code UNKNOWN_SIZE}.
     */
    public static int getLength(Object obj) {
        if (obj == null)
            return 0;
        else if (obj instanceof Collection)
            return ((Collection<?>)obj).size();
        else if (obj instanceof CharSequence)
            return ((CharSequence)obj).length();
        else if (obj.getClass().isArray())
            return Array.getLength(obj);

        return UNKNOWN_SIZE;
    }

    /**
     * Gets iterator or create new for container object with elements with unknown type.
     *
     * @param obj Container object with elements.
     * @return Iterator.
     */
    public static Iterator<?> getIterator(final Object obj) {
        assert obj != null;

        if (obj instanceof Iterable)
            return ((Iterable<?>)obj).iterator();
        else if (obj instanceof Enumeration) {
            final Enumeration<?> enumeration = (Enumeration<?>)obj;

            return new Iterator<Object>() {
                @Override public boolean hasNext() {
                    return enumeration.hasMoreElements();
                }

                @Override public Object next() {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    return enumeration.nextElement();
                }

                @Override public void remove() {
                    throw new UnsupportedOperationException("Not implemented.");
                }
            };
        }
        else if (obj instanceof Iterator)
            return (Iterator<?>)obj;
        else if (obj instanceof CharSequence) {
            final CharSequence cSeq = (CharSequence)obj;

            return new Iterator<Object>() {
                private int idx;

                @Override public boolean hasNext() {
                    return idx < cSeq.length();
                }

                @Override public Object next() {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    idx++;

                    return cSeq.charAt(idx - 1);
                }

                @Override public void remove() {
                    throw new UnsupportedOperationException("Not implemented.");
                }
            };
        }
        else if (obj.getClass().isArray()) {
            return new Iterator<Object>() {
                private int idx;

                @Override public boolean hasNext() {
                    return idx < Array.getLength(obj);
                }

                @Override public Object next() {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    idx++;

                    return Array.get(obj, idx - 1);
                }

                @Override public void remove() {
                    throw new UnsupportedOperationException("Not implemented.");
                }
            };
        }

        throw new IllegalArgumentException("Unknown parameter type: " + obj.getClass().getName());
    }

    /**
     * Check if method return type can be used for {@link GridifySetToSet}
     * or {@link GridifySetToValue} annotations.
     *
     * @param cls Method return type class to check.
     * @return {@code true} if method return type is valid.
     */
    public static boolean isMethodReturnTypeValid(Class<?> cls) {
        for (Class<?> mtdReturnType : ALLOWED_MTD_RETURN_TYPES) {
            if (mtdReturnType.equals(cls))
                return true;
        }

        return cls.isArray();
    }

    /**
     * Check if method parameter type type can be used for {@link GridifySetToSet}
     * or {@link GridifySetToValue} annotations.
     *
     * @param cls Method parameter to check.
     * @return {@code true} if method parameter type is valid.
     */
    @SuppressWarnings({"UnusedCatchParameter"})
    public static boolean isMethodParameterTypeAllowed(Class<?> cls) {
        for (Class<?> mtdReturnType : ALLOWED_MTD_PARAM_TYPES) {
            if (mtdReturnType.equals(cls))
                return true;
        }

        if (cls.isArray())
            return true;

        int mod = cls.getModifiers();

        if (!Modifier.isInterface(mod) && !Modifier.isAbstract(mod) && Collection.class.isAssignableFrom(cls)) {
            Constructor[] ctors = cls.getConstructors();

            for (Constructor ctor : ctors) {
                try {
                    if (ctor.getParameterTypes().length == 0 && ctor.newInstance() != null)
                        return true;
                }
                catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
                    // No-op.
                }
            }
        }


        return false;
    }

    /**
     * Check is method parameter annotated with {@link org.apache.ignite.compute.gridify.GridifyInput}.
     *
     * @param anns Annotations for method parameters.
     * @return {@code true} if annotation found.
     */
    public static boolean isMethodParameterTypeAnnotated(Annotation[] anns) {
        if (anns != null && anns.length > 0) {
            for (Annotation ann : anns) {
                if (ann.annotationType().equals(GridifyInput.class))
                    return true;
            }
        }

        return false;
    }

    /**
     * Allowed method return types.
     *
     * @return List of class names or text comments.
     */
    public static List<String> getAllowedMethodReturnTypes() {
        List<String> types = new ArrayList<>(ALLOWED_MTD_RETURN_TYPES.length + 1);

        for (Class<?> type : ALLOWED_MTD_RETURN_TYPES) {
            types.add(type.getName());
        }

        types.add("Java Array");

        return types;
    }

    /**
     * Allowed method return types.
     *
     * @return List of class names or text comments.
     */
    public static Collection<String> getAllowedMethodParameterTypes() {
        Collection<String> types = new ArrayList<>(ALLOWED_MTD_PARAM_TYPES.length + 1);

        for (Class<?> type : ALLOWED_MTD_PARAM_TYPES) {
            types.add(type.getName());
        }

        types.add("Java Array");

        return Collections.unmodifiableCollection(types);
    }

    /**
     * Converts parameter object to {@link Collection}.
     *
     * @param arg Method parameter object.
     * @return Collection of parameters or {@code null} for unknown object.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable
    public static Collection parameterToCollection(Object arg) {
        if (arg instanceof Collection)
            return (Collection)arg;
        else if (arg instanceof Iterator) {
            Collection res = new ArrayList();

            for (Iterator iter = (Iterator)arg; iter.hasNext();)
                res.add(iter.next());

            return res;
        }
        else if (arg instanceof Iterable) {
            Collection res = new ArrayList();

            for (Object o : ((Iterable)arg)) {
                res.add(o);
            }

            return res;
        }
        else if (arg instanceof Enumeration) {
            Collection res = new ArrayList();

            Enumeration elements = (Enumeration)arg;

            while (elements.hasMoreElements())
                res.add(elements.nextElement());

            return res;
        }
        else if (arg != null && arg.getClass().isArray()) {
            Collection res = new ArrayList();

            for (int i = 0; i < Array.getLength(arg); i++)
                res.add(Array.get(arg, i));

            return res;
        }
        else if (arg instanceof CharSequence) {
            CharSequence elements = (CharSequence)arg;

            Collection<Character> res = new ArrayList<>(elements.length());

            for (int i = 0; i < elements.length(); i++)
                res.add(elements.charAt(i));

            return res;
        }

        return null;
    }

    /**
     * Converts {@link Collection} back to object applied for method.
     *
     * @param paramCls Method parameter type.
     * @param data Collection of data elements.
     * @return Object applied for method.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable
    public static Object collectionToParameter(Class<?> paramCls, Collection data) {
        if (Collection.class.equals(paramCls))
            return data;
        else if (Iterable.class.equals(paramCls))
            return data;
        else if (Iterator.class.equals(paramCls))
            return new IteratorAdapter(data);
        else if (Enumeration.class.equals(paramCls))
            return new EnumerationAdapter(data);
        else if (Set.class.equals(paramCls))
            return new HashSet(data);
        else if (List.class.equals(paramCls))
            return new LinkedList(data);
        else if (Queue.class.equals(paramCls))
            return new LinkedList(data);
        else if (CharSequence.class.equals(paramCls)) {
            SB sb = new SB();

            for (Object obj : data) {
                assert obj instanceof Character;

                sb.a(obj);
            }

            return sb;
        }
        else if (paramCls.isArray()) {
            Class<?> componentType = paramCls.getComponentType();

            Object arr = Array.newInstance(componentType, data.size());

            int i = 0;

            for (Object element : data) {
                Array.set(arr, i, element);

                i++;
            }

            return arr;
        }
        // Note, that parameter class must contain default non-param constructor.
        else if (Collection.class.isAssignableFrom(paramCls)) {
            try {
                Collection col = (Collection)paramCls.newInstance();

                for (Object dataObj : data) {
                    col.add(dataObj);
                }

                return col;
            }
            catch (InstantiationException | IllegalAccessException ignored) {
                // No-op.
            }
        }

        return null;
    }

    /**
     * Serializable {@link Enumeration} implementation based on {@link Collection}.
     */
    private static class EnumerationAdapter<T> implements Enumeration<T>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private Collection<T> col;

        /** */
        private transient Iterator<T> iter;

        /**
         * Creates enumeration.
         *
         * @param col Collection.
         */
        private EnumerationAdapter(Collection<T> col) {
            this.col = col;

            iter = col.iterator();
        }

        /** {@inheritDoc} */
        @Override public boolean hasMoreElements() {
            return iter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public T nextElement() {
            return iter.next();
        }

        /**
         * Recreate inner state for object after deserialization.
         *
         * @param in Input stream.
         * @throws ClassNotFoundException Thrown in case of error.
         * @throws IOException Thrown in case of error.
         */
        private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
            // Always perform the default de-serialization first.
            in.defaultReadObject();

            iter = col.iterator();
        }

        /**
         * @param out Output stream
         * @throws IOException Thrown in case of error.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            // Perform the default serialization for all non-transient, non-static fields.
            out.defaultWriteObject();
        }
    }

    /**
     * Serializable {@link Iterator} implementation based on {@link Collection}.
     */
    private static class IteratorAdapter<T> implements Iterator<T>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private Collection<T> col;

        /** */
        private transient Iterator<T> iter;

        /**
         * @param col Collection.
         */
        IteratorAdapter(Collection<T> col) {
            this.col = col;

            iter = col.iterator();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return iter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public T next() {
            return iter.next();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            iter.remove();
        }

        /**
         * Recreate inner state for object after deserialization.
         *
         * @param in Input stream.
         * @throws ClassNotFoundException Thrown in case of error.
         * @throws IOException Thrown in case of error.
         */
        private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
            // Always perform the default de-serialization first.
            in.defaultReadObject();

            iter = col.iterator();
        }

        /**
         * @param out Output stream
         * @throws IOException Thrown in case of error.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            // Perform the default serialization for all non-transient, non-static fields.
            out.defaultWriteObject();
        }
    }
}