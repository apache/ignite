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

package org.apache.ignite.spi.checkpoint.sharedfs;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.*;

import java.io.*;

/**
 * Utility class that helps to manage files. It provides read/write
 * methods that simplify file operations.
 */
final class SharedFsUtils {
    /**
     * Enforces singleton.
     */
    private SharedFsUtils() {
        // No-op.
    }

    /**
     * Reads all checkpoint data from given file. File is read as binary
     * data which is than deserialized.
     *
     * @param file File which contains checkpoint data.
     * @param m Grid marshaller.
     * @param log Messages logger.
     * @return Checkpoint data object read from given file.
     * @throws IgniteCheckedException Thrown if data could not be converted
     *    to {@link SharedFsCheckpointData} object.
     * @throws IOException Thrown if file read error occurred.
     */
    static SharedFsCheckpointData read(File file, Marshaller m, IgniteLogger log)
        throws IOException, IgniteCheckedException {
        assert file != null;
        assert m != null;
        assert log != null;

        InputStream in = new FileInputStream(file);

        try {
            return (SharedFsCheckpointData)m.unmarshal(in, U.gridClassLoader());
        }
        finally {
            U.close(in, log);
        }
    }

    /**
     * Writes given checkpoint data to a given file. Data are serialized to
     * the binary stream and saved to the file.
     *
     * @param file File data should be saved to.
     * @param data Checkpoint data.
     * @param m Grid marshaller.
     * @param log Messages logger.
     * @throws IgniteCheckedException Thrown if data could not be marshalled.
     * @throws IOException Thrown if file write operation failed.
     */
    static void write(File file, SharedFsCheckpointData data, Marshaller m, IgniteLogger log)
        throws IOException, IgniteCheckedException {
        assert file != null;
        assert m != null;
        assert data != null;
        assert log != null;

        OutputStream out = null;

        try {
            out = new FileOutputStream(file);

            m.marshal(data, out);
        }
        finally {
            U.close(out, log);
        }
    }
}
