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

package org.apache.ignite.spi.deployment.uri.scanners.file;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.deployment.uri.scanners.GridDeploymentFileHandler;
import org.apache.ignite.spi.deployment.uri.scanners.GridDeploymentFolderScannerHelper;
import org.apache.ignite.spi.deployment.uri.scanners.UriDeploymentScanner;
import org.apache.ignite.spi.deployment.uri.scanners.UriDeploymentScannerContext;

/**
 * URI deployment file scanner.
 */
public class UriDeploymentFileScanner implements UriDeploymentScanner {
    /** Default scan frequency. */
    public static final int DFLT_SCAN_FREQ = 5000;

    /** Per-URI contexts. */
    private final ConcurrentHashMap<URI, URIContext> uriCtxs = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public boolean acceptsURI(URI uri) {
        String proto = uri.getScheme().toLowerCase();

        return "file".equals(proto);
    }

    /** {@inheritDoc} */
    @Override public void scan(UriDeploymentScannerContext scanCtx) {
        URI uri = scanCtx.getUri();

        URIContext uriCtx = uriCtxs.get(uri);

        if (uriCtx == null) {
            uriCtx = createUriContext(uri, scanCtx);

            URIContext oldUriCtx = uriCtxs.putIfAbsent(uri, uriCtx);

            if (oldUriCtx != null)
                uriCtx = oldUriCtx;
        }

        uriCtx.scan(scanCtx);
    }

    /** {@inheritDoc} */
    @Override public long getDefaultScanFrequency() {
        return DFLT_SCAN_FREQ;
    }

    /**
     * Create context for the given URI.
     *
     * @param uri URI.
     * @param scanCtx Scanner context.
     * @return URI context.
     */
    private URIContext createUriContext(URI uri, final UriDeploymentScannerContext scanCtx) {
        String scanDirPath = uri.getPath();

        File scanDir = null;

        if (scanDirPath != null)
            scanDir = new File(scanDirPath);

        if (scanDir == null || !scanDir.isDirectory())
            throw new IgniteSpiException("URI is either not provided or is not a directory: " +
                U.hidePassword(uri.toString()));

        FileFilter garFilter = new FileFilter() {
            /** {@inheritDoc} */
            @Override public boolean accept(File pathname) {
                return scanCtx.getFilter().accept(null, pathname.getName());
            }
        };

        FileFilter garDirFilesFilter = new FileFilter() {
            /** {@inheritDoc} */
            @Override public boolean accept(File pathname) {
                // Allow all files in GAR-directory.
                return pathname.isFile();
            }
        };

        return new URIContext(scanDir, garFilter, garDirFilesFilter);
    }

    /**
     * Converts given file name to the URI with "file" scheme.
     *
     * @param name File name to be converted.
     * @return File name with "file://" prefix.
     */
    private static String getFileUri(String name) {
        assert name != null;

        name = name.replace("\\","/");

        return "file://" + (name.charAt(0) == '/' ? "" : '/') + name;
    }

    /**
     * Context for the given URI.
     */
    private static class URIContext {
        /** Scanning directory or file. */
        private final File scanDir;

        /** GAR filter. */
        private final FileFilter garFilter;

        /** GAR directory files filter. */
        private final FileFilter garDirFilesFilter;

        /** Cache of found GAR-files or GAR-directories to check if any of it has been updated. */
        private final Map<File, Long> tstampCache = new HashMap<>();

        /** Cache of found files in GAR-folder to check if any of it has been updated. */
        private final Map<File, Map<File, Long>> garDirFilesTstampCache = new HashMap<>();

        /**
         * Constructor.
         *
         * @param scanDir Scan directory.
         * @param garFilter Gar filter.
         * @param garDirFilesFilter GAR directory files filter.
         */
        private URIContext(File scanDir, FileFilter garFilter, FileFilter garDirFilesFilter) {
            this.scanDir = scanDir;
            this.garFilter = garFilter;
            this.garDirFilesFilter = garDirFilesFilter;
        }

        /**
         * Perform scan.
         *
         * @param scanCtx Scan context.
         */
        private void scan(final UriDeploymentScannerContext scanCtx) {
            final Set<File> foundFiles = scanCtx.isFirstScan() ?
                new HashSet<File>() : U.<File>newHashSet(tstampCache.size());

            GridDeploymentFileHandler hnd = new GridDeploymentFileHandler() {
                /** {@inheritDoc} */
                @Override public void handle(File file) {
                    foundFiles.add(file);

                    handleFile(file, scanCtx);
                }
            };

            // Scan directory for deploy units.
            GridDeploymentFolderScannerHelper.scanFolder(scanDir, garFilter, hnd);

            // Print warning if no GAR-units found first time.
            if (scanCtx.isFirstScan() && foundFiles.isEmpty())
                U.warn(scanCtx.getLogger(), "No GAR-units found in: " + U.hidePassword(scanCtx.getUri().toString()));

            if (!scanCtx.isFirstScan()) {
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

                    scanCtx.getListener().onDeletedFiles(uris);
                }
            }
        }

        /**
         * Tests whether given directory or file was changed since last check and if so
         * copies all directory sub-folders and files or file itself to the deployment
         * directory and than notifies listener about new or updated files.
         *
         * @param file Scanning directory or file.
         * @param ctx Scanner context.
         */
        private void handleFile(File file, UriDeploymentScannerContext ctx) {
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

                if (ctx.getLogger().isDebugEnabled())
                    ctx.getLogger().debug("Discovered deployment file or directory: " + file);

                String fileName = file.getName();

                try {
                    File cpFile = ctx.createTempFile(fileName, ctx.getDeployDirectory());

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

                    assert lastMod != null;

                    ctx.getListener().onNewOrUpdatedFile(cpFile, fileUri, lastMod);
                }
                catch (IOException e) {
                    U.error(ctx.getLogger(), "Error saving file: " + fileName, e);
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
        @SuppressWarnings("ConstantConditions")
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
    }
}