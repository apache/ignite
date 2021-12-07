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

/**
 * Serialization type flags.
 */
public class SerializationType {
    /** Used for predefined descriptors like primitive (or boxed int). See {@link DefaultType}. */
    public static final int DEFAULT = 0;

    /** Type for classes that are neither serializable nor externalizable.  */
    public static final int ARBITRARY = 1;

    /** Externalizable. */
    public static final int EXTERNALIZABLE = 1 << 1;

    /** Simple serializable. */
    public static final int SERIALIZABLE = 1 << 2;

    /** Serializable with readObject/writeObject flag. */
    public static final int SERIALIZABLE_OVERRIDE = 1 << 3;

    /** Serializable with writeReplace flag. */
    public static final int SERIALIZABLE_WRITE_REPLACE = 1 << 4;

    /** Serializable with readResolve flag. */
    public static final int SERIALIZABLE_READ_RESOLVE = 1 << 5;
}
