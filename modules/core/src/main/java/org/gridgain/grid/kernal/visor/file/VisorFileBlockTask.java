/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.file;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Task to read file block.
 */
@GridInternal
public class VisorFileBlockTask extends VisorOneNodeTask<VisorFileBlockTask.VisorFileBlockArg,
    IgniteBiTuple<? extends IOException, VisorFileBlock>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorFileBlockJob job(VisorFileBlockArg arg) {
        return new VisorFileBlockJob(arg);
    }

    /**
     * Arguments for {@link VisorFileBlockTask}
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorFileBlockArg implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Log file path. */
        private final String path;

        /** Log file offset. */
        private final long offset;

        /** Block size. */
        private final int blockSz;

        /** Log file last modified timestamp. */
        private final long lastModified;

        /**
         * @param path Log file path.
         * @param offset Offset in file.
         * @param blockSz Block size.
         * @param lastModified Log file last modified timestamp.
         */
        public VisorFileBlockArg(String path, long offset, int blockSz, long lastModified) {
            this.path = path;
            this.offset = offset;
            this.blockSz = blockSz;
            this.lastModified = lastModified;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorFileBlockArg.class, this);
        }
    }

    /**
     * Job that read file block on node.
     */
    private static class VisorFileBlockJob
        extends VisorJob<VisorFileBlockArg, IgniteBiTuple<? extends IOException, VisorFileBlock>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Descriptor of file block to read.
         */
        private VisorFileBlockJob(VisorFileBlockArg arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<? extends IOException, VisorFileBlock> run(VisorFileBlockArg arg) throws GridException {
            try {
                URL url = U.resolveGridGainUrl(arg.path);

                if (url == null)
                    return new IgniteBiTuple<>(new NoSuchFileException("File path not found: " + arg.path), null);

                VisorFileBlock block = readBlock(new File(url.toURI()), arg.offset, arg.blockSz, arg.lastModified);

                return new IgniteBiTuple<>(null, block);
            }
            catch (IOException e) {
                return new IgniteBiTuple<>(e, null);
            }
            catch (URISyntaxException ignored) {
                return new IgniteBiTuple<>(new NoSuchFileException("File path not found: " + arg.path), null);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorFileBlockJob.class, this);
        }
    }
}
