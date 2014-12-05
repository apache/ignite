/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri.scanners.ftp;

import com.enterprisedt.net.ftp.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.util.*;

/**
 * Value object which encapsulates {@link FTPFile} and corresponding directory.
 */
class GridUriDeploymentFtpFile {
    /** */
    private final String dir;

    /** */
    private final FTPFile file;

    /**
     * @param dir Remote FTP directory.
     * @param file FTP file.
     */
    GridUriDeploymentFtpFile(String dir, FTPFile file) {
        assert dir != null;
        assert file != null;
        assert file.getName() != null;

        this.dir = dir;
        this.file = file;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof GridUriDeploymentFtpFile))
            return false;

        GridUriDeploymentFtpFile other = (GridUriDeploymentFtpFile)obj;

        return dir.equals(other.dir) && file.getName().equals(other.file.getName());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = dir.hashCode();

        res = 29 * res + file.getName().hashCode();

        return res;
    }

    /**
     * @return File name.
     */
    String getName() {
        return file.getName();
    }

    /**
     * @return Last modification date as calendar.
     */
    Calendar getTimestamp() {
        Date date = file.lastModified();

        Calendar cal = null;

        if (date != null) {
            cal = Calendar.getInstance();

            cal.setTime(date);
        }

        return cal;
    }

    /**
     * @return {@code True} in case provided file is a directory.
     */
    boolean isDirectory() {
        return file.isDir();
    }

    /**
     * @return {@code True} in case provided file is not a directory or link.
     */
    boolean isFile() {
        return !file.isDir() && !file.isLink();
    }

    /**
     * @return File parent directory.
     */
    String getParentDirectory() {
        return dir;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentFtpFile.class, this);
    }
}
