/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.jdbc;


import org.apache.ignite.mbean.*;
import org.gridgain.grid.spi.*;

/**
 * Management bean that provides general administrative and configuration information
 * about jdbc checkpoint SPI.
 */
@IgniteMBeanDescription("MBean that provides information about jdbc checkpoint SPI.")
public interface GridJdbcCheckpointSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets number of retries in case of DB failure.
     *
     * @return Number of retries.
     */
    @IgniteMBeanDescription("Number of retries.")
    public int getNumberOfRetries();

    /**
     * Gets data source description.
     *
     * @return Description for data source.
     */
    @IgniteMBeanDescription("Data source description.")
    public String getDataSourceInfo();

    /**
     * Gets checkpoint jdbc user name.
     *
     * @return User name for checkpoint jdbc.
     */
    @IgniteMBeanDescription("User name for checkpoint database.")
    public String getUser();

    /**
     * Gets checkpoint jdbc password.
     *
     * @return Password for checkpoint jdbc.
     */
    @IgniteMBeanDescription("Password for checkpoint database.")
    public String getPwd();

    /**
     * Gets checkpoint table name.
     *
     * @return Checkpoint table name.
     */
    @IgniteMBeanDescription("Checkpoint table name.")
    public String getCheckpointTableName();

    /**
     * Gets key field name for checkpoint table.
     *
     * @return Key field name for checkpoint table.
     */
    @IgniteMBeanDescription("Key field name for checkpoint table.")
    public String getKeyFieldName();

    /**
     * Gets key field type for checkpoint table.
     *
     * @return Key field type for checkpoint table.
     */
    @IgniteMBeanDescription("Key field type for checkpoint table.")
    public String getKeyFieldType();

    /**
     * Gets value field name for checkpoint table.
     *
     * @return Value field name for checkpoint table.
     */
    @IgniteMBeanDescription("Value field name for checkpoint table.")
    public String getValueFieldName();

    /**
     * Gets value field type for checkpoint table.
     *
     * @return Value field type for checkpoint table.
     */
    @IgniteMBeanDescription("Value field type for checkpoint table.")
    public String getValueFieldType();

    /**
     * Gets expiration date field name for checkpoint table.
     *
     * @return Create date field name for checkpoint table.
     */
    @IgniteMBeanDescription("Expiration date field name for checkpoint table.")
    public String getExpireDateFieldName();

    /**
     * Gets expiration date field type for checkpoint table.
     *
     * @return Expiration date field type for checkpoint table.
     */
    @IgniteMBeanDescription("Expiration date field type for checkpoint table.")
    public String getExpireDateFieldType();
}
