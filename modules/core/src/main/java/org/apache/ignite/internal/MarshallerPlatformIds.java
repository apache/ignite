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

package org.apache.ignite.internal;

/**
 * Constants for platform IDs to feed into {@link org.apache.ignite.marshaller.MarshallerContext}.
 */
public final class MarshallerPlatformIds {
    /** */
    public static final byte JAVA_ID = 0;

    /** */
    public static final byte DOTNET_ID = 1;

    /**
     * Gets the platform name by id.
     *
     * @param platformId Id.
     * @return Name.
     */
    public static String platformName(byte platformId) {
        switch (platformId) {
            case JAVA_ID: return "Java";
            case DOTNET_ID: return ".NET";
        }

        return "Unknown";
    }

    /**
     * Gets all known platform ids except the specified one.
     *
     * @param platformId Id.
     * @return Other ids.
     */
    public static byte[] otherPlatforms(byte platformId) {
        switch (platformId) {
            case JAVA_ID: return new byte[]{DOTNET_ID};
            case DOTNET_ID: return new byte[]{JAVA_ID};
        }

        return new byte[]{JAVA_ID, DOTNET_ID};
    }

    /** */
    private MarshallerPlatformIds() {
        // No-op.
    }
}
