/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.file;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.kernal.visor.log.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Get list files matching filter.
 */
@GridInternal
public class VisorLatestTextFilesTask extends VisorOneNodeTask<IgniteBiTuple<String, String>, Collection<VisorLogFile>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorLatestTextFilesJob job(IgniteBiTuple<String, String> arg) {
        return new VisorLatestTextFilesJob(arg, debug);
    }

    /**
     * Job that gets list of files.
     */
    private static class VisorLatestTextFilesJob extends VisorJob<IgniteBiTuple<String, String>, Collection<VisorLogFile>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Folder and regexp.
         * @param debug Debug flag.
         */
        private VisorLatestTextFilesJob(IgniteBiTuple<String, String> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Nullable @Override protected Collection<VisorLogFile> run(final IgniteBiTuple<String, String> arg) throws GridException {
            String path = arg.get1();
            String regexp = arg.get2();

            assert path != null;
            assert regexp != null;

            URL url = U.resolveGridGainUrl(path);

            if (url == null)
                return null;

            try {
                File folder = new File(url.toURI());

                List<VisorLogFile> files = matchedFiles(folder, regexp);

                if (files.isEmpty())
                    return null;

                if (files.size() > LOG_FILES_COUNT_LIMIT)
                    files = new ArrayList<>(files.subList(0, LOG_FILES_COUNT_LIMIT));

                return files;
            }
            catch (Exception ignored) {
                return null;
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorLatestTextFilesJob.class, this);
        }
    }
}
