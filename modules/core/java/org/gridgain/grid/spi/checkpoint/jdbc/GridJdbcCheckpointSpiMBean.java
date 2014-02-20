// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.jdbc;


import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.mbean.*;

/**
 * Management bean that provides general administrative and configuration information
 * about jdbc checkpoint SPI.
 *
 * @author @java.author
 * @version @java.version
 */
@GridMBeanDescription("MBean that provides information about jdbc checkpoint SPI.")
public interface GridJdbcCheckpointSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets number of retries in case of DB failure.
     *
     * @return Number of retries.
     */
    @GridMBeanDescription("Number of retries.")
    public int getNumberOfRetries();

    /**
     * Gets data source description.
     *
     * @return Description for data source.
     */
    @GridMBeanDescription("Data source description.")
    public String getDataSourceInfo();

    /**
     * Gets checkpoint jdbc user name.
     *
     * @return User name for checkpoint jdbc.
     */
    @GridMBeanDescription("User name for checkpoint database.")
    public String getUser();

    /**
     * Gets checkpoint jdbc password.
     *
     * @return Password for checkpoint jdbc.
     */
    @GridMBeanDescription("Password for checkpoint database.")
    public String getPwd();

    /**
     * Gets checkpoint table name.
     *
     * @return Checkpoint table name.
     */
    @GridMBeanDescription("Checkpoint table name.")
    public String getCheckpointTableName();

    /**
     * Gets key field name for checkpoint table.
     *
     * @return Key field name for checkpoint table.
     */
    @GridMBeanDescription("Key field name for checkpoint table.")
    public String getKeyFieldName();

    /**
     * Gets key field type for checkpoint table.
     *
     * @return Key field type for checkpoint table.
     */
    @GridMBeanDescription("Key field type for checkpoint table.")
    public String getKeyFieldType();

    /**
     * Gets value field name for checkpoint table.
     *
     * @return Value field name for checkpoint table.
     */
    @GridMBeanDescription("Value field name for checkpoint table.")
    public String getValueFieldName();

    /**
     * Gets value field type for checkpoint table.
     *
     * @return Value field type for checkpoint table.
     */
    @GridMBeanDescription("Value field type for checkpoint table.")
    public String getValueFieldType();

    /**
     * Gets expiration date field name for checkpoint table.
     *
     * @return Create date field name for checkpoint table.
     */
    @GridMBeanDescription("Expiration date field name for checkpoint table.")
    public String getExpireDateFieldName();

    /**
     * Gets expiration date field type for checkpoint table.
     *
     * @return Expiration date field type for checkpoint table.
     */
    @GridMBeanDescription("Expiration date field type for checkpoint table.")
    public String getExpireDateFieldType();
}
