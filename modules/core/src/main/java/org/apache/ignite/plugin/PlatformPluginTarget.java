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

package org.apache.ignite.plugin;

import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;

/**
 * Platform plugin target: interface that is invoked from platform code to perform operations.
 */
public interface PlatformPluginTarget {
    /**
     * Invokes operation.
     *
     * @param opCode Operation code.
     * @param reader Reader.
     * @param writer Writer.
     * @param arg Optional argument.
     * @param context Plugin context.
     *
     * @throws IgniteException In case of error.
     */
    PlatformPluginTarget invokeOperation(int opCode, BinaryRawReader reader, BinaryRawWriter writer,
        PlatformPluginTarget arg, PlatformPluginContext context)
        throws IgniteException;
}
