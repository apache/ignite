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

package org.apache.ignite.yarn.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import static org.apache.hadoop.yarn.api.ApplicationConstants.LOG_DIR_EXPANSION_VAR;

/**
 * Utils.
 */
public class IgniteYarnUtils {
    /** */
    public static final String DEFAULT_IGNITE_CONFIG = "ignite-default-config.xml";

    /** */
    public static final String SPACE = " ";

    /** */
    public static final String JAR_NAME = "ignite-yarn.jar";

    /** */
    public static final String YARN_LOG_OUT =
        " 1>" + LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + LOG_DIR_EXPANSION_VAR + "/stderr";

    /**
     * @param file Path.
     * @param fs File system.
     * @param type Local resource type.
     * @throws Exception If failed.
     */
    public static LocalResource setupFile(Path file, FileSystem fs, LocalResourceType type)
        throws Exception {
        LocalResource resource = Records.newRecord(LocalResource.class);

        file = fs.makeQualified(file);

        FileStatus stat = fs.getFileStatus(file);

        resource.setResource(ConverterUtils.getYarnUrlFromPath(file));
        resource.setSize(stat.getLen());
        resource.setTimestamp(stat.getModificationTime());
        resource.setType(type);
        resource.setVisibility(LocalResourceVisibility.APPLICATION);

        return resource;
    }

    /**
     * @param fs File system.
     * @param src Source path.
     * @param dst Destination path.
     * @return Path to file to hdfs file system.
     */
    public static Path copyLocalToHdfs(FileSystem fs, String src, String dst) throws Exception {
        Path dstPath = new Path(dst);

        // Local file isn't removed, dst file override.
        fs.copyFromLocalFile(false, true, new Path(src), dstPath);

        return dstPath;
    }

    /**
     * Creates a ByteBuffer with serialized {@link Credentials}.
     *
     * @param creds The credentials.
     * @return The ByteBuffer with the credentials.
     * @throws IOException
     */
    public static ByteBuffer createTokenBuffer(Credentials creds) throws IOException {
        DataOutputBuffer dob = new DataOutputBuffer();

        creds.writeTokenStorageToStream(dob);

        return ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }
}