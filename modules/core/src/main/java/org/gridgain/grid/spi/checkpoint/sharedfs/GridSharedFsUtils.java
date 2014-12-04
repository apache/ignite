/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.sharedfs;

import org.apache.ignite.*;
import org.apache.ignite.marshaller.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.io.*;

/**
 * Utility class that helps to manage files. It provides read/write
 * methods that simplify file operations.
 */
final class GridSharedFsUtils {
    /**
     * Enforces singleton.
     */
    private GridSharedFsUtils() {
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
     * @throws GridException Thrown if data could not be converted
     *    to {@link GridSharedFsCheckpointData} object.
     * @throws IOException Thrown if file read error occurred.
     */
    static GridSharedFsCheckpointData read(File file, GridMarshaller m, IgniteLogger log)
        throws IOException, GridException {
        assert file != null;
        assert m != null;
        assert log != null;

        InputStream in = new FileInputStream(file);

        try {
            return (GridSharedFsCheckpointData)m.unmarshal(in, U.gridClassLoader());
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
     * @throws GridException Thrown if data could not be marshalled.
     * @throws IOException Thrown if file write operation failed.
     */
    static void write(File file, GridSharedFsCheckpointData data, GridMarshaller m, IgniteLogger log)
        throws IOException, GridException {
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
