/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri;

import org.apache.ignite.compute.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.io.*;
import java.util.*;

/**
 * Deployment file processing result.
 */
class GridUriDeploymentFileProcessorResult {
    /** Class loader. */
    private ClassLoader clsLdr;

    /** Task class. */
    private List<Class<? extends ComputeTask<?, ?>>> taskClss;

    /** Deploument unit file. */
    private File file;

    /** Hash of deployment unit. */
    private String md5;

    /**
     * Getter for property 'clsLdr'.
     *
     * @return Value for property 'clsLdr'.
     */
    public ClassLoader getClassLoader() {
        return clsLdr;
    }

    /**
     * Setter for property 'clsLdr'.
     *
     * @param clsLdr Value to set for property 'clsLdr'.
     */
    public void setClassLoader(ClassLoader clsLdr) {
        this.clsLdr = clsLdr;
    }

    /**
     * Getter for property 'taskClss'.
     *
     * @return Value for property 'taskClss'.
     */
    public List<Class<? extends ComputeTask<?, ?>>> getTaskClasses() {
        return taskClss;
    }

    /**
     * Setter for property 'taskClss'.
     *
     * @param taskClss Value to set for property 'taskClss'.
     */
    public void setTaskClasses(List<Class<? extends ComputeTask<?, ?>>> taskClss) {
        this.taskClss = taskClss;
    }

    /**
     * Gets GAR file.
     *
     * @return GAR file.
     */
    public File getFile() {
        return file;
    }

    /**
     * Sets GAR file.
     *
     * @param file GAR file.
     */
    public void setFile(File file) {
        this.file = file;
    }

    /**
     * @return md5 hash of this deployment result.
     */
    public String getMd5() {
        return md5;
    }

    /**
     * @param md5 md5 hash of this deployment result.
     */
    public void setMd5(String md5) {
        this.md5 = md5;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentFileProcessorResult.class, this);
    }
}
