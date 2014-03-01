/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.segmentation.sharedfs;

import org.gridgain.grid.*;
import org.gridgain.grid.segmentation.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Segmentation resolver implementation that checks whether
 * node is in the correct segment or not by writing to and reading from
 * shared directory.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridSharedFsSegmentationResolver implements GridSegmentationResolver {
    /** Path to shared directory. */
    private String path;

    /** Shared folder. */
    private File folder;

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override public boolean isValidSegment() throws GridException {
        init();

        return folder.canRead() && folder.canWrite();
    }

    /**
     * Initializes checker.
     *
     * @throws GridException If failed.
     */
    private void init() throws GridException {
        if (initGuard.compareAndSet(false, true)) {
            try {
                if (path == null)
                    throw new GridSpiException("Shared file system path is null " +
                        "(it should be configured via setPath(..) configuration property).");

                URL folderUrl = U.resolveGridGainUrl(path);

                if (folderUrl == null)
                    throw new GridException("Failed to resolve path: " + path);

                File tmp;

                try {
                    tmp = new File(folderUrl.toURI());
                }
                catch (URISyntaxException e) {
                    throw new GridException("Failed to resolve path: " + path, e);
                }

                folder = tmp;
            }
            finally {
                initLatch.countDown();
            }
        }
        else {
            U.await(initLatch);

            if (folder == null)
                throw new GridException("Segmentation resolver was not properly initialized.");
        }
    }

    /**
     * Sets path to shared folder.
     * <p>
     * This is required property.
     *
     * @param path Path to shared folder.
     */
    public void setPath(String path) {
        this.path = path;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSharedFsSegmentationResolver.class, this);
    }
}
