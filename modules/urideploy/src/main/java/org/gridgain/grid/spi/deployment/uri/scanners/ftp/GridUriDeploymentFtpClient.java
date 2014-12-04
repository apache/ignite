/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri.scanners.ftp;

import com.enterprisedt.net.ftp.*;
import org.apache.ignite.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.io.*;
import java.text.*;
import java.util.*;

/**
 * URI FTP deployment client.
 */
class GridUriDeploymentFtpClient {
    /** Timeout in milliseconds on the underlying socket. */
    private static final int TIMEOUT = 60000;

    /** */
    private final GridUriDeploymentFtpConfiguration cfg;

    /** */
    private final IgniteLogger log;

    /** */
    private FTPClient ftp;

    /** */
    private boolean isConnected;

    /**
     * @param cfg FTP configuration.
     * @param log Logger to use.
     */
    GridUriDeploymentFtpClient(GridUriDeploymentFtpConfiguration cfg, IgniteLogger log) {
        assert cfg != null;
        assert log != null;

        this.cfg = cfg;
        this.log = log;
    }


    /**
     * @param rmtFile Remote file.
     * @param localFile Local file.
     * @throws GridUriDeploymentFtpException Thrown in case of any error.
     */
    void downloadToFile(GridUriDeploymentFtpFile rmtFile, File localFile) throws GridUriDeploymentFtpException {
        assert ftp != null;
        assert rmtFile != null;
        assert localFile != null;

        String dirName = rmtFile.getParentDirectory();

        if (dirName.length() == 0 || '/' != dirName.charAt(dirName.length() - 1))
            dirName += '/';

        String srcPath = dirName + rmtFile.getName();

        try {
            BufferedOutputStream out = null;

            try {
                out = new BufferedOutputStream(new FileOutputStream(localFile));

                ftp.get(out, srcPath);
            }
            finally {
                U.close(out, log);
            }
        }
        catch (IOException | FTPException e) {
            throw new GridUriDeploymentFtpException("Failed to download file [rmtFile=" + srcPath + ", localFile=" +
                localFile + ']', e);
        }
    }

    /**
     * @throws GridUriDeploymentFtpException Thrown in case of any error.
     */
    void connect() throws GridUriDeploymentFtpException {
        ftp = new FTPClient();

        try {
            ftp.setRemoteHost(cfg.getHost());
            ftp.setRemotePort(cfg.getPort());

            // Set socket timeout to avoid an infinite timeout.
            ftp.setTimeout(TIMEOUT);

            ftp.connect();

            ftp.login(cfg.getUsername(), cfg.getPassword());

            // Set up passive binary transfers.
            ftp.setConnectMode(FTPConnectMode.PASV);
            ftp.setType(FTPTransferType.BINARY);

            if (!ftp.connected()) {
                ftp.quit();

                throw new GridUriDeploymentFtpException("FTP server refused connection [host=" + cfg.getHost() +
                    ", port=" + cfg.getPort() + ", username=" + cfg.getUsername() + ']');
            }
        }
        catch (IOException | FTPException e) {
            throw new GridUriDeploymentFtpException("Failed to connect to host [host=" + cfg.getHost() +
                ", port=" + cfg.getPort() + ']', e);
        }

        isConnected = true;
    }

    /**
     * @throws GridUriDeploymentFtpException Thrown in case of any error.
     */
    void close() throws GridUriDeploymentFtpException {
        if (!isConnected)
            return;

        assert ftp != null;

        Exception e = null;

        try {
            ftp.quit();
        }
        catch (IOException e1) {
            e = e1;
        }
        catch (FTPException e1) {
            e = e1;
        }
        finally{
            if (ftp.connected()) {
                try {
                    ftp.quit();
                }
                catch (IOException | FTPException e1) {
                    // Don't loose the initial exception.
                    if (e == null)
                        e = e1;
                }
            }
        }

        ftp = null;

        isConnected = false;

        if (e != null)
            throw new GridUriDeploymentFtpException("Failed to close FTP client.", e);
    }

    /**
     * @return List of files.
     * @throws GridUriDeploymentFtpException Thrown in case of any error.
     */
    List<GridUriDeploymentFtpFile> getFiles() throws GridUriDeploymentFtpException {
        try {
            assert cfg.getDirectory() != null;

            List<GridUriDeploymentFtpFile> clientFiles = new ArrayList<>();

            FTPFile[] files = ftp.dirDetails(cfg.getDirectory());

            for (FTPFile file : files) {
                clientFiles.add(new GridUriDeploymentFtpFile(cfg.getDirectory(), file));
            }

            return clientFiles;
        }
        catch (IOException | ParseException | FTPException e) {
            throw new GridUriDeploymentFtpException("Failed to get files in directory: " + cfg.getDirectory(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentFtpClient.class, this);
    }
}
