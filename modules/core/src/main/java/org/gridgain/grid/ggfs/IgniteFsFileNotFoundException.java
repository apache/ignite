/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

/**
 * {@code GGFS} exception indicating that target resource is not found.
 */
public class IgniteFsFileNotFoundException extends IgniteFsInvalidPathException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates exception with error message specified.
     *
     * @param msg Error message.
     */
    public IgniteFsFileNotFoundException(String msg) {
        super(msg);
    }

    /**
     * Creates exception with given exception cause.
     *
     * @param cause Exception cause.
     */
    public IgniteFsFileNotFoundException(Throwable cause) {
        super(cause);
    }
}
