// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.security;

import org.gridgain.grid.security.*;

import java.io.*;

/**
 * Security context.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridSecurityContext implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Security subject. */
    private GridSecuritySubject subj;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridSecurityContext() {
        // No-op.
    }

    /**
     * @param subj Subject.
     */
    public GridSecurityContext(GridSecuritySubject subj) {
        this.subj = subj;

        // TODO init permissions rules.
    }

    /**
     * @return Security subject.
     */
    public GridSecuritySubject subject() {
        return subj;
    }

    /**
     * Checks whether task operation is allowed.
     *
     * @param taskClsName Task class name.
     * @param perm Permission to check.
     * @return {@code True} if task operation is allowed.
     */
    public boolean taskOperationAllowed(String taskClsName, GridSecurityPermission perm) {
        return true;
    }

    /**
     * Checks whether cache operation is allowed.
     *
     * @param cacheName Cache name.
     * @param perm Permission to check.
     * @return {@code True} if cache operation is allowed.
     */
    public boolean cacheOperationAllowed(String cacheName, GridSecurityPermission perm) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(subj);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        subj = (GridSecuritySubject)in.readObject();
    }
}
