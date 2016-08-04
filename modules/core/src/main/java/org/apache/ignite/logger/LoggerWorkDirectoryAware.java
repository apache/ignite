package org.apache.ignite.logger;

/**
 * Interface for Ignite file appenders to change work directory path.
 */
public interface LoggerWorkDirectoryAware {

    /**
     * Sets Work directory.
     *
     * @param workDir Work directory.
     */
    void setWorkDir(String workDir);

    /**
     * Gets  Work directory.
     *
     * @return Work directory.
     */
    String getWorkDir();
}
