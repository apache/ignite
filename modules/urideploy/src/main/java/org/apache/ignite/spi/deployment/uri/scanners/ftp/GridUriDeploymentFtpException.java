/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri.scanners.ftp;

import org.apache.ignite.*;

/**
 * An exception occurred during URI FTP deployment.
 */
class GridUriDeploymentFtpException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new grid exception with given error message.
     *
     * @param msg Error message.
     */
    GridUriDeploymentFtpException(String msg) { super(msg); }

    /**
     * Creates new grid ftp client exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    GridUriDeploymentFtpException(String msg, Throwable cause) { super(msg, cause); }
}
