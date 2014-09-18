/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * External task metadata (classpath, JVM options) needed to start external process execution.
 */
public class GridHadoopExternalTaskMetadata {
    /** Process classpath. */
    private Collection<String> classpath;

    /** JVM options. */
    @GridToStringInclude
    private Collection<String> jvmOpts;

    /**
     * @return JVM Options.
     */
    public Collection<String> jvmOptions() {
        return jvmOpts;
    }

    /**
     * @param jvmOpts JVM options.
     */
    public void jvmOptions(Collection<String> jvmOpts) {
        this.jvmOpts = jvmOpts;
    }

    /**
     * @return Classpath.
     */
    public Collection<String> classpath() {
        return classpath;
    }

    /**
     * @param classpath Classpath.
     */
    public void classpath(Collection<String> classpath) {
        this.classpath = classpath;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopExternalTaskMetadata.class, this);
    }
}
