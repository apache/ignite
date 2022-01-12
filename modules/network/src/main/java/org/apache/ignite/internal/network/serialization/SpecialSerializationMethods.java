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

package org.apache.ignite.internal.network.serialization;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Encapsulates special serialization methods like writeReplace()/readResolve() for convenient invocation.
 */
public interface SpecialSerializationMethods {
    /**
     * Invokes {@code writeReplace()} on the target object. Should only be used if the target descriptor supports
     * the method (that is, its serialization type is
     * {@link org.apache.ignite.internal.network.serialization.SerializationType#SERIALIZABLE} or
     * {@link org.apache.ignite.internal.network.serialization.SerializationType#EXTERNALIZABLE} and the class
     * actually has writeReplace() method).
     * If any of these conditions fail, a {@link NullPointerException} will be thrown.
     *
     * @param object target object on which to invoke the method
     * @return invocation result
     * @throws SpecialMethodInvocationException if the invocation fails
     */
    Object writeReplace(Object object) throws SpecialMethodInvocationException;

    /**
     * Invokes {@code readResolve()} on the target object. Should only be used if the target descriptor supports
     * the method (that is, its serialization type is
     * {@link org.apache.ignite.internal.network.serialization.SerializationType#SERIALIZABLE} or
     * {@link org.apache.ignite.internal.network.serialization.SerializationType#EXTERNALIZABLE} and the class
     * actually has readResolve() method).
     * If any of these conditions fail, a {@link NullPointerException} will be thrown.
     *
     * @param object target object on which to invoke the method
     * @return invocation result
     * @throws SpecialMethodInvocationException if the invocation fails
     */
    Object readResolve(Object object) throws SpecialMethodInvocationException;

    /**
     * Invokes {@code writeObject()} on the target object. Should only be used if the target descriptor supports
     * the method (that is, its serialization type is
     * {@link org.apache.ignite.internal.network.serialization.SerializationType#SERIALIZABLE} and the class
     * actually has writeObject() method).
     * If any of these conditions fail, a {@link NullPointerException} will be thrown.
     *
     * @param object target object on which to invoke the method
     * @param stream stream to pass to the method
     * @throws SpecialMethodInvocationException if the invocation fails
     */
    void writeObject(Object object, ObjectOutputStream stream) throws SpecialMethodInvocationException;

    /**
     * Invokes {@code readObject()} on the target object. Should only be used if the target descriptor supports
     * the method (that is, its serialization type is
     * {@link org.apache.ignite.internal.network.serialization.SerializationType#SERIALIZABLE} and the class
     * actually has readObject() method).
     * If any of these conditions fail, a {@link NullPointerException} will be thrown.
     *
     * @param object target object on which to invoke the method
     * @param stream stream to pass to the method
     * @throws SpecialMethodInvocationException if the invocation fails
     */
    void readObject(Object object, ObjectInputStream stream) throws SpecialMethodInvocationException;
}
