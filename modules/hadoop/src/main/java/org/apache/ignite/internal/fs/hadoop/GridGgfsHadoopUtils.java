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

package org.apache.ignite.internal.fs.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.ignite.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.internal.processors.fs.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Utility constants and methods for GGFS Hadoop file system.
 */
public class GridGgfsHadoopUtils {
    /** Parameter name for endpoint no embed mode flag. */
    public static final String PARAM_GGFS_ENDPOINT_NO_EMBED = "fs.ggfs.%s.endpoint.no_embed";

    /** Parameter name for endpoint no shared memory flag. */
    public static final String PARAM_GGFS_ENDPOINT_NO_LOCAL_SHMEM = "fs.ggfs.%s.endpoint.no_local_shmem";

    /** Parameter name for endpoint no local TCP flag. */
    public static final String PARAM_GGFS_ENDPOINT_NO_LOCAL_TCP = "fs.ggfs.%s.endpoint.no_local_tcp";

    /**
     * Get string parameter.
     *
     * @param cfg Configuration.
     * @param name Parameter name.
     * @param authority Authority.
     * @param dflt Default value.
     * @return String value.
     */
    public static String parameter(Configuration cfg, String name, String authority, String dflt) {
        return cfg.get(String.format(name, authority != null ? authority : ""), dflt);
    }

    /**
     * Get integer parameter.
     *
     * @param cfg Configuration.
     * @param name Parameter name.
     * @param authority Authority.
     * @param dflt Default value.
     * @return Integer value.
     * @throws IOException In case of parse exception.
     */
    public static int parameter(Configuration cfg, String name, String authority, int dflt) throws IOException {
        String name0 = String.format(name, authority != null ? authority : "");

        try {
            return cfg.getInt(name0, dflt);
        }
        catch (NumberFormatException ignore) {
            throw new IOException("Failed to parse parameter value to integer: " + name0);
        }
    }

    /**
     * Get boolean parameter.
     *
     * @param cfg Configuration.
     * @param name Parameter name.
     * @param authority Authority.
     * @param dflt Default value.
     * @return Boolean value.
     */
    public static boolean parameter(Configuration cfg, String name, String authority, boolean dflt) {
        return cfg.getBoolean(String.format(name, authority != null ? authority : ""), dflt);
    }

    /**
     * Cast GG exception to appropriate IO exception.
     *
     * @param e Exception to cast.
     * @return Casted exception.
     */
    public static IOException cast(IgniteCheckedException e) {
        return cast(e, null);
    }

    /**
     * Cast GG exception to appropriate IO exception.
     *
     * @param e Exception to cast.
     * @param path Path for exceptions.
     * @return Casted exception.
     */
    @SuppressWarnings("unchecked")
    public static IOException cast(IgniteCheckedException e, @Nullable String path) {
        assert e != null;

        // First check for any nested IOException; if exists - re-throw it.
        if (e.hasCause(IOException.class))
            return e.getCause(IOException.class);
        else if (e.getCause() instanceof IgniteFsFileNotFoundException)
            return new FileNotFoundException(path); // TODO: Or PathNotFoundException?
        else if (e.getCause() instanceof IgniteFsParentNotDirectoryException)
            return new ParentNotDirectoryException(path);
        else if (path != null && e.getCause() instanceof GridGgfsDirectoryNotEmptyException)
            return new PathIsNotEmptyDirectoryException(path);
        else if (path != null && e.getCause() instanceof IgniteFsPathAlreadyExistsException)
            return new PathExistsException(path);
        else
            return new IOException(e);
    }

    /**
     * Constructor.
     */
    private GridGgfsHadoopUtils() {
        // No-op.
    }
}
