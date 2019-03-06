/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.checkpoint.sharedfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;

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
            return U.unmarshal(m, in, U.gridClassLoader());
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

            U.marshal(m, data, out);
        }
        finally {
            U.close(out, log);
        }
    }
}