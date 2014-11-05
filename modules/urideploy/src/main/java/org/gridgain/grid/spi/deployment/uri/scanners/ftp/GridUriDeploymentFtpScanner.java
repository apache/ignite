/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri.scanners.ftp;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.spi.deployment.uri.scanners.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * FTP scanner scans directory for new files. Scanned directory defined in URI.
 * Scanner doesn't search files in subfolders.
 */
public class GridUriDeploymentFtpScanner extends GridUriDeploymentScanner {
    /** */
    private static final long UNKNOWN_FILE_TSTAMP = -1;

    /** */
    private final GridUriDeploymentFtpConfiguration cfg;

    /** Cache of found files to check if any of it has been updated. */
    private Map<GridUriDeploymentFtpFile, Long> cache = new HashMap<>();

    /**
     * @param gridName Grid instance name.
     * @param uri FTP URI.
     * @param deployDir FTP directory.
     * @param freq Scanner frequency.
     * @param filter Scanner filter.
     * @param lsnr Deployment listener.
     * @param log Logger to use.
     */
    public GridUriDeploymentFtpScanner(
        String gridName,
        URI uri,
        File deployDir,
        long freq,
        FilenameFilter filter,
        GridUriDeploymentScannerListener lsnr,
        GridLogger log) {
        super(gridName, uri, deployDir, freq, filter, lsnr, log);

        cfg = initializeFtpConfiguration(uri);
   }

    /**
     * @param uri FTP URI.
     * @return FTP configuration.
     */
    private GridUriDeploymentFtpConfiguration initializeFtpConfiguration(URI uri) {
        assert "ftp".equals(uri.getScheme());

        GridUriDeploymentFtpConfiguration cfg = new GridUriDeploymentFtpConfiguration();

        String userInfo = uri.getUserInfo();
        String username = null;
        String pswd = null;

        if (userInfo != null) {
            String[] arr = userInfo.split(";");

            if (arr != null && arr.length > 0)
                for (String el : arr)
                    if (el.startsWith("freq=")) {
                        // No-op.
                    }
                    else if (el.indexOf(':') != -1) {
                        int idx = el.indexOf(':');

                        username = el.substring(0, idx);
                        pswd = el.substring(idx + 1);
                    }
                    else
                        username = el;
        }

        // Username and password must be defined in URI.
        if (username == null)
            throw new GridRuntimeException("Username has not been provided.");

        if (pswd == null)
            throw new GridRuntimeException("Password has not been provided.");

        cfg.setHost(uri.getHost());
        cfg.setPort(uri.getPort());
        cfg.setUsername(username);
        cfg.setPassword(pswd);
        cfg.setDirectory(uri.getPath());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void process() {
        Collection<GridUriDeploymentFtpFile> foundFiles = U.newHashSet(cache.size());

        long start = U.currentTimeMillis();

        processFtp(foundFiles);

        if (getLogger().isDebugEnabled())
            getLogger().debug("FTP scanner time in milliseconds: " + (U.currentTimeMillis() - start));

        if (!isFirstScan()) {
            Collection<GridUriDeploymentFtpFile> delFiles = new HashSet<>(cache.keySet());

            delFiles.removeAll(foundFiles);

            if (!delFiles.isEmpty()) {
                List<String> uris = new ArrayList<>();

                for (GridUriDeploymentFtpFile file : delFiles) {
                    Long tstamp = cache.get(file);

                    // Ignore files in cache w/o timestamp.
                    if (tstamp != null && tstamp != UNKNOWN_FILE_TSTAMP)
                        uris.add(getFileUri(file.getName()));
                }

                cache.keySet().removeAll(delFiles);

                getListener().onDeletedFiles(uris);
            }
        }
    }

    /**
     * @param files File to process.
     */
    @SuppressWarnings({"UnusedCatchParameter"})
    private void processFtp(Collection<GridUriDeploymentFtpFile> files) {
        GridUriDeploymentFtpClient ftp = new GridUriDeploymentFtpClient(cfg, getLogger());

        try {
            ftp.connect();

            for (GridUriDeploymentFtpFile file : ftp.getFiles()) {
                String fileName = file.getName();

                if (getFilter().accept(null, fileName.toLowerCase()) && file.isFile()) {
                    files.add(file);

                    Long lastModified = cache.get(file);

                    Calendar fileTstamp = file.getTimestamp();

                    if (fileTstamp == null) {
                        if (lastModified == null) {
                            // Add new file in cache to avoid print warning every time.
                            cache.put(file, UNKNOWN_FILE_TSTAMP);

                            U.warn(getLogger(), "File with unknown timestamp will be ignored " +
                                "(check FTP server configuration): " + file);
                        }
                    }
                    // If file is new or has been modified.
                    else if (lastModified == null || lastModified != fileTstamp.getTimeInMillis()) {
                        cache.put(file, fileTstamp.getTimeInMillis());

                        if (getLogger().isDebugEnabled())
                            getLogger().debug("Discovered deployment file or directory: " + file);

                        try {
                            File diskFile = createTempFile(fileName, getDeployDirectory());

                            ftp.downloadToFile(file, diskFile);

                            String fileUri = getFileUri(fileName);

                            // Delete file when JVM stopped.
                            diskFile.deleteOnExit();

                            // Deployment SPI apply.
                            // NOTE: If SPI listener blocks then FTP connection may be closed by timeout.
                            getListener().onNewOrUpdatedFile(diskFile, fileUri, fileTstamp.getTimeInMillis());
                        }
                        catch (IOException e) {
                            U.error(getLogger(), "Failed to download file from FTP server: " + fileName, e);
                        }
                    }
                }
            }
        }
        catch (GridUriDeploymentFtpException e) {
            if (!isCancelled()) {
                String maskedUri = getUri() != null ? U.hidePassword(getUri().toString()) : null;

                if (e.hasCause(ConnectException.class))
                    LT.warn(getLogger(), e, "Failed to connect to FTP server (connection refused): " + maskedUri);

                else if (e.hasCause(UnknownHostException.class))
                    LT.warn(getLogger(), e, "Failed to connect to FTP server (host is unknown): " + maskedUri);

                else
                    U.error(getLogger(), "Failed to get files from FTP server: " + maskedUri, e);
            }
        }
        finally {
            try {
                ftp.close();
            }
            catch (GridUriDeploymentFtpException e) {
                if (!isCancelled())
                    U.error(getLogger(), "Failed to close FTP client.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentFtpScanner.class, this,
            "uri", getUri() != null ? U.hidePassword(getUri().toString()) : null,
            "freq", getFrequency(),
            "deployDir", getDeployDirectory());
    }
}
