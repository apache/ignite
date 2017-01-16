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

package org.apache.ignite.internal.suggestions;

/**
 * Java Virtual Machine options.
 */
class JvmOptions {
    static final String SERVER = "-server";
    static final String USE_TLAB = "-XX:+UseTLAB";
    static final String DISABLE_EXPLICIT_GC = "-XX:+DisableExplicitGC";
    static final String USE_COMPRESSED_OOPS = "-XX:+UseCompressedOops";
    static final String MAX_DIRECT_MEMORY_SIZE = "-XX:MaxDirectMemorySize";
    static final String XMX = "-Xmx";
}
