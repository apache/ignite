/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.checkpoint.jdbc;


import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;

/**
 * Management bean that provides general administrative and configuration information
 * about jdbc checkpoint SPI.
 */
@MXBeanDescription("MBean that provides information about jdbc checkpoint SPI.")
public interface JdbcCheckpointSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets number of retries in case of DB failure.
     *
     * @return Number of retries.
     */
    @MXBeanDescription("Number of retries.")
    public int getNumberOfRetries();

    /**
     * Gets data source description.
     *
     * @return Description for data source.
     */
    @MXBeanDescription("Data source description.")
    public String getDataSourceInfo();

    /**
     * Gets checkpoint jdbc user name.
     *
     * @return User name for checkpoint jdbc.
     */
    @MXBeanDescription("User name for checkpoint database.")
    public String getUser();

    /**
     * Gets checkpoint jdbc password.
     *
     * @return Password for checkpoint jdbc.
     */
    @MXBeanDescription("Password for checkpoint database.")
    public String getPwd();

    /**
     * Gets checkpoint table name.
     *
     * @return Checkpoint table name.
     */
    @MXBeanDescription("Checkpoint table name.")
    public String getCheckpointTableName();

    /**
     * Gets key field name for checkpoint table.
     *
     * @return Key field name for checkpoint table.
     */
    @MXBeanDescription("Key field name for checkpoint table.")
    public String getKeyFieldName();

    /**
     * Gets key field type for checkpoint table.
     *
     * @return Key field type for checkpoint table.
     */
    @MXBeanDescription("Key field type for checkpoint table.")
    public String getKeyFieldType();

    /**
     * Gets value field name for checkpoint table.
     *
     * @return Value field name for checkpoint table.
     */
    @MXBeanDescription("Value field name for checkpoint table.")
    public String getValueFieldName();

    /**
     * Gets value field type for checkpoint table.
     *
     * @return Value field type for checkpoint table.
     */
    @MXBeanDescription("Value field type for checkpoint table.")
    public String getValueFieldType();

    /**
     * Gets expiration date field name for checkpoint table.
     *
     * @return Create date field name for checkpoint table.
     */
    @MXBeanDescription("Expiration date field name for checkpoint table.")
    public String getExpireDateFieldName();

    /**
     * Gets expiration date field type for checkpoint table.
     *
     * @return Expiration date field type for checkpoint table.
     */
    @MXBeanDescription("Expiration date field type for checkpoint table.")
    public String getExpireDateFieldType();
}