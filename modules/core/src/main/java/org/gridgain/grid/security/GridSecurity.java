/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.security;

import java.util.*;

/**
 * Grid security facade.
 */
public interface GridSecurity {
    /**
     * Gets collection of authorized subjects.
     *
     * @return Collection of authorized subjects.
     */
    public Collection<GridSecuritySubject> authenticatedSubjects();

    /**
     * Gets security subject by subject type and subject ID.
     *
     * @param subjType Subject type.
     * @param subjId Subject ID.
     * @return Authorized security subject.
     */
    public GridSecuritySubject authenticatedSubject(GridSecuritySubjectType subjType, UUID subjId);
}
