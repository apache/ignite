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

package org.apache.ignite.marshaller;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

import java.io.InputStream;

/**
 * Utility marshaller methods.
 */
public class MarshallerUtils {
    /**
     * Set node name to marshaller context if possible.
     *
     * @param marsh Marshaller instance.
     * @param nodeName Node name.
     * @return Marshaller instance.
     */
    public static Marshaller withNodeName(Marshaller marsh, @Nullable String nodeName) {
        if (marsh instanceof AbstractNodeNameAwareMarshaller)
            ((AbstractNodeNameAwareMarshaller)marsh).nodeName(nodeName);

        return marsh;
    }

    /**
     * Unmarshal object and set grid name thread local.
     *
     * @param name Grid name.
     * @param marsh Marshaller.
     * @param arr Binary data.
     * @param ldr Class loader.
     * @return Deserialized object.
     * @throws IgniteCheckedException If failed.
     */
    public static <T> T unmarshal(String name, Marshaller marsh, byte[] arr, @Nullable ClassLoader ldr)
        throws IgniteCheckedException {
        String oldName = IgniteUtils.setCurrentIgniteName(name);

        try {
            return marsh.unmarshal(arr, ldr);
        }
        finally {
            IgniteUtils.restoreCurrentIgniteName(oldName);
        }
    }

    /**
     * Private constructor.
     */
    private MarshallerUtils() {
        // No-op.
    }
}
