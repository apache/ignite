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
 * IDs of built-in descriptors. Only defines part of IDs which are needed directly in constant expressions (like switch);
 * most of them are defined directly on enum members in {@link BuiltInType}.
 */
public class BuiltInTypeIds {
    public static final int BYTE = 0;
    public static final int SHORT = 2;
    public static final int INT = 4;
    public static final int FLOAT = 6;
    public static final int LONG = 8;
    public static final int DOUBLE = 10;
    public static final int BOOLEAN = 12;
    public static final int CHAR = 14;
}
