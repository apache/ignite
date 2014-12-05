/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri.scanners.file;

import org.apache.ignite.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.spi.deployment.uri.scanners.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Scanner that processes all URIs with "file" scheme. Usually URI point to
 * certain directory or file and scanner is in charge of watching all changes
 * (file deletion, creation and so on) and sending notification to the listener
 * about every change.
 */
public class GridUriDeploymentFileScanner extends GridUriDeploymentScanner {
    /** Scanning directory or file. */
    private File scanDir;

    /** Cache of found GAR-files or GAR-directories to check if any of it has been updated. */
    private Map<File, Long> tstampCache = new HashMap<>();

    /** Cache of found files in GAR-folder to check if any of it has been updated. */
    private Map<File, Map<File, Long>> garDirFilesTstampCache = new HashMap<>();

    /** */
    private FileFilter garFilter;

    /** */
    private FileFilter garDirFilesFilter;

    /**
     * Creates new instance of scanner with given name.
     *
     * @param gridName Grid name.
     * @param uri URI which scanner should look after.
     * @param deployDir Temporary deployment directory.
     * @param freq Scan frequency.
     * @param filter Found files filter.
     * @param lsnr Scanner listener which should be notifier about changes.
     * @param log Logger.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown if URI is {@code null} or is not a
     *      directory.
     */
    public GridUriDeploymentFileScanner(
        String gridName,
        URI uri,
        File deployDir,
        long freq,
        FilenameFilter filter,
        GridUriDeploymentScannerListener lsnr,
        IgniteLogger log) throws IgniteSpiException {
        super(gridName, uri, deployDir, freq, filter, lsnr, log);

        initialize(uri);
    }

    /**
     * Initializes scanner by parsing given URI and extracting scanning
     * directory path and creating file filters.
     *
     * @param uri Scanning URI with "file" scheme.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown if URI is {@code null} or is not a
     *      directory.
     */
    private void initialize(URI uri) throws IgniteSpiException {
        assert "file".equals(getUri().getScheme());

        String scanDirPath = uri.getPath();

        if (scanDirPath != null)
            scanDir = new File(scanDirPath);

        if (scanDir == null || !scanDir.isDirectory()) {
            scanDir = null;

            throw new IgniteSpiException("URI is either not provided or is not a directory: " +
                U.hidePassword(uri.toString()));
        }

        garFilter = new FileFilter() {
            /** {@inheritDoc} */
            @Override public boolean accept(File pathname) {
                return getFilter().accept(null, pathname.getName());
            }
        };

        garDirFilesFilter = new FileFilter() {
            /** {@inheritDoc} */
            @Override public boolean accept(File pathname) {
                // Allow all files in GAR-directory.
                return pathname.isFile();
            }
        };
    }

    /**
     * Handles changes in scanning directory by tracking files modification date.
     * Checks files modification date against those one that was collected before
     * and notifies listener about every changed or deleted file.
     */
    @Override protected void process() {
        final Set<File> foundFiles = isFirstScan() ? new HashSet<File>() : U.<File>newHashSet(tstampCache.size());

        GridDeploymentFileHandler hnd = new GridDeploymentFileHandler() {
            /** {@inheritDoc} */
            @Override public void handle(File file) {
                foundFiles.add(file);

                handleFile(file);
            }
        };

        // Scan directory for deploy units.
        GridDeploymentFolderScannerHelper.scanFolder(scanDir, garFilter, hnd);

        // Print warning if no GAR-units found first time.
        if (isFirstScan() && foundFiles.isEmpty())
            U.warn(getLogger(), "No GAR-units found in: " + U.hidePassword(getUri().toString()));

        if (!isFirstScan()) {
            Collection<File> deletedFiles = new HashSet<>(tstampCache.keySet());

            deletedFiles.removeAll(foundFiles);

            if (!deletedFiles.isEmpty()) {
                List<String> uris = new ArrayList<>();

                for (File file : deletedFiles) {
                    uris.add(getFileUri(file.getAbsolutePath()));
                }

                // Clear cache.
                tstampCache.keySet().removeAll(deletedFiles);

                garDirFilesTstampCache.keySet().removeAll(deletedFiles);

                getListener().onDeletedFiles(uris);
            }
        }
    }

    /**
     * Tests whether given directory or file was changed since last check and if so
     * copies all directory sub-folders and files or file itself to the deployment
     * directory and than notifies listener about new or updated files.
     *
     * @param file Scanning directory or file.
     */
    private void handleFile(File file) {
        boolean changed;

        Long lastMod;

        if (file.isDirectory()) {
            GridTuple<Long> dirLastModified = F.t(file.lastModified());

            changed = checkGarDirectoryChanged(file, dirLastModified);

            lastMod = dirLastModified.get();
        }
        else {
            lastMod = tstampCache.get(file);

            changed = lastMod == null || lastMod != file.lastModified();

            lastMod = file.lastModified();
        }

        // If file is new or has been modified.
        if (changed) {
            tstampCache.put(file, lastMod);

            if (getLogger().isDebugEnabled())
                getLogger().debug("Discovered deployment file or directory: " + file);

            String fileName = file.getName();

            try {
                File cpFile = createTempFile(fileName, getDeployDirectory());

                // Delete file when JVM stopped.
                cpFile.deleteOnExit();

                if (file.isDirectory()) {
                    cpFile = new File(cpFile.getParent(), "dir_" + cpFile.getName());

                    // Delete directory when JVM stopped.
                    cpFile.deleteOnExit();
                }

                // Copy file to deploy directory.
                U.copy(file, cpFile, true);

                String fileUri = getFileUri(file.getAbsolutePath());

                getListener().onNewOrUpdatedFile(cpFile, fileUri, lastMod);
            }
            catch (IOException e) {
                U.error(getLogger(), "Error saving file: " + fileName, e);
            }
        }
    }

    /**
     * Tests whether certain directory was changed since given modification date.
     * It scans all directory files one by one and compares their modification
     * dates with those ones that was collected before.
     * <p>
     * If at least one file was changed (has modification date after given one)
     * whole directory is considered as modified.
     *
     * @param dir Scanning directory.
     * @param lastModified Last calculated Directory modification date.
     * @return {@code true} if directory was changed since last check and
     *      {@code false} otherwise.
     */
    private boolean checkGarDirectoryChanged(File dir, final GridTuple<Long> lastModified) {
        final Map<File, Long> clssTstampCache;

        boolean firstScan = false;

        if (!garDirFilesTstampCache.containsKey(dir)) {
            firstScan = true;

            garDirFilesTstampCache.put(dir, clssTstampCache = new HashMap<>());
        }
        else
            clssTstampCache = garDirFilesTstampCache.get(dir);

        assert clssTstampCache != null;

        final GridTuple<Boolean> changed = F.t(false);

        final Set<File> foundFiles = firstScan ? new HashSet<File>() : U.<File>newHashSet(clssTstampCache.size());

        GridDeploymentFileHandler hnd = new GridDeploymentFileHandler() {
            @Override public void handle(File file) {
                foundFiles.add(file);

                Long fileLastModified = clssTstampCache.get(file);

                if (fileLastModified == null || fileLastModified != file.lastModified()) {
                    clssTstampCache.put(file, fileLastModified = file.lastModified());

                    changed.set(true);
                }

                // Calculate last modified file in folder.
                if (fileLastModified > lastModified.get())
                    lastModified.set(fileLastModified);
            }
        };

        // Scan GAR-directory for changes.
        GridDeploymentFolderScannerHelper.scanFolder(dir, garDirFilesFilter, hnd);

        // Clear cache for deleted files.
        if (!firstScan && clssTstampCache.keySet().retainAll(foundFiles))
            changed.set(true);

        return changed.get();
    }

    /**
     * Converts given file name to the URI with "file" scheme.
     *
     * @param name File name to be converted.
     * @return File name with "file://" prefix.
     */
    @Override protected String getFileUri(String name) {
        assert name != null;

        name = name.replace("\\","/");

        return "file://" + (name.charAt(0) == '/' ? "" : '/') + name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append(getClass().getName()).append(" [");
        buf.append("scanDir=").append(scanDir);
        buf.append(']');

        return buf.toString();
    }
}

