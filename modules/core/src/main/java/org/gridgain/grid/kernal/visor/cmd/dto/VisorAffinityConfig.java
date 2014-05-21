/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import java.io.*;

/**
 * Affinity configuration data.
 */
public class VisorAffinityConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final String affinity;
    private final String affinityMapper;

    public VisorAffinityConfig(String affinity, String affinityMapper) {
        this.affinity = affinity;
        this.affinityMapper = affinityMapper;
    }

    /**
     * @return Affinity.
     */
    public String affinity() {
        return affinity;
    }

    /**
     * @return Affinity mapper.
     */
    public String affinityMapper() {
        return affinityMapper;
    }
}
