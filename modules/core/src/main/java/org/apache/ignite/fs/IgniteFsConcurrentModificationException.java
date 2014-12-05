/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.fs;

/**
 * {@code GGFS} exception indicating that file system structure was modified concurrently. This error
 * indicates that an operation performed in DUAL mode cannot proceed due to these changes.
 */
public class IgniteFsConcurrentModificationException extends IgniteFsException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new exception.
     *
     * @param path Affected path.
     */
    public IgniteFsConcurrentModificationException(IgniteFsPath path) {
        super("File system entry has been modified concurrently: " + path, null);
    }
}
