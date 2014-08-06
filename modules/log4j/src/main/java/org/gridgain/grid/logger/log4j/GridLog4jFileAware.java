/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger.log4j;

import org.gridgain.grid.lang.*;

/**
 * Interface for those loggers and appenders that evaluate their file paths lazily.
 */
interface GridLog4jFileAware {
    /**
     * Sets closure that later evaluate file path.
     *
     * @param filePathClos Closure that generates actual file path.
     */
    void updateFilePath(GridClosure<String, String> filePathClos);
}
