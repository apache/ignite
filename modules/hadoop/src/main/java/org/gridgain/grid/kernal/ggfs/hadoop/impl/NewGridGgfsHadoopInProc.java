/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

import org.apache.commons.logging.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.ggfs.hadoop.impl.NewGridGgfsHadoopMode.*;

/**
 * Communication with grid in the same process.
 */
public class NewGridGgfsHadoopInProc implements NewGridGgfsHadoopEx { // TODO: Remove abstract.
    /** Target GGFS. */
    private final GridGgfsEx ggfs;

    /** Buffer size. */
    private final int bufSize;

    /** Logger. */
    private final Log log;

    /**
     * COnstructor.
     *
     * @param ggfs Target GGFS.
     */
    public NewGridGgfsHadoopInProc(GridGgfsEx ggfs, Log log) {
        this.ggfs = ggfs;
        this.log = log;

        bufSize = ggfs.configuration().getBlockSize() * 2;
    }

    /** {@inheritDoc} */
    @Override public GridGgfsHandshakeResponse handshake(String logDir) {
        // TODO.

        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // TODO.
    }

    /** {@inheritDoc} */
    @Override public GridGgfsFile info(GridGgfsPath path) throws GridException {
        try {
            return ggfs.info(path);
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to get file info because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public GridGgfsFile update(GridGgfsPath path, Map<String, String> props) throws GridException {
        try {
            return ggfs.update(path, props);
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to update file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean setTimes(GridGgfsPath path, long accessTime, long modificationTime) throws GridException {
        try {
            ggfs.setTimes(path, accessTime, modificationTime);

            return true;
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to set path times because Grid is stopping: " +
                path);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean rename(GridGgfsPath src, GridGgfsPath dest) throws GridException {
        try {
            ggfs.rename(src, dest);

            return true;
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to rename path because Grid is stopping: " + src);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean delete(GridGgfsPath path, boolean recursive) throws GridException {
        try {
            return ggfs.delete(path, recursive);
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to delete path because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public GridGgfsStatus fsStatus() throws GridException {
        try {
            return ggfs.globalSpace();
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to get file system status because Grid is " +
                "stopping.");
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsPath> listPaths(GridGgfsPath path) throws GridException {
        try {
            return ggfs.listPaths(path);
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to list paths because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsFile> listFiles(GridGgfsPath path) throws GridException {
        try {
            return ggfs.listFiles(path);
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to list files because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean mkdirs(GridGgfsPath path, Map<String, String> props) throws GridException {
        try {
            ggfs.mkdirs(path, props);

            return true;
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to create directory because Grid is stopping: " +
                path);
        }
    }

    /** {@inheritDoc} */
    @Override public GridGgfsPathSummary contentSummary(GridGgfsPath path) throws GridException {
        try {
            return ggfs.summary(path);
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to get content summary because Grid is stopping: " +
                path);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsBlockLocation> affinity(GridGgfsPath path, long start, long len)
        throws GridException {
        try {
            return ggfs.affinity(path, start, len);
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to get affinity because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate open(GridGgfsPath path) throws GridException {
        try {
            GridGgfsInputStreamAdapter stream = ggfs.open(path, bufSize);

            return new NewGridGgfsHadoopStreamDelegate(this, stream, stream.fileInfo().length());
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to open file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate open(GridGgfsPath path, int seqReadsBeforePrefetch)
        throws GridException {
        try {
            GridGgfsInputStreamAdapter stream = ggfs.open(path, bufSize, seqReadsBeforePrefetch);

            return new NewGridGgfsHadoopStreamDelegate(this, stream, stream.fileInfo().length());
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to open file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate create(GridGgfsPath path, boolean overwrite, boolean colocate,
        int replication, long blockSize, @Nullable Map<String, String> props) throws GridException {
        try {
            GridGgfsOutputStream stream = ggfs.create(path, bufSize, overwrite,
                colocate ? ggfs.nextAffinityKey() : null, replication, blockSize, props);

            return new NewGridGgfsHadoopStreamDelegate(this, stream);
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to create file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate append(GridGgfsPath path, boolean create,
        @Nullable Map<String, String> props) throws GridException {
        try {
            GridGgfsOutputStream stream = ggfs.append(path, bufSize, create, props);

            return new NewGridGgfsHadoopStreamDelegate(this, stream);
        }
        catch (IllegalStateException e) {
            throw new GridGgfsHadoopCommunicationException("Failed to append file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public GridPlainFuture<byte[]> readData(NewGridGgfsHadoopStreamDelegate desc, long pos, int len,
        @Nullable byte[] outBuf, int outOff, int outLen) {
        GridGgfsInputStreamAdapter stream = desc.target();

        // TODO.

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeData(NewGridGgfsHadoopStreamDelegate desc, byte[] data, int off, int len)
        throws GridException, IOException {
        GridGgfsOutputStream stream = desc.target();

        stream.write(data, off, len);
    }

    /** {@inheritDoc} */
    @Override public Boolean closeStream(NewGridGgfsHadoopStreamDelegate desc) throws GridException, IOException {
        Closeable closeable = desc.target();

        closeable.close();

        return true;
    }

    /** {@inheritDoc} */
    @Override public void addEventListener(NewGridGgfsHadoopStreamDelegate desc, GridGgfsHadoopStreamEventListener lsnr) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void removeEventListener(NewGridGgfsHadoopStreamDelegate desc) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopMode mode() {
        return IN_PROC;
    }
}
