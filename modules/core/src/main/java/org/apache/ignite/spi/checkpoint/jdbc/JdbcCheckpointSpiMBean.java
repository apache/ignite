/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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