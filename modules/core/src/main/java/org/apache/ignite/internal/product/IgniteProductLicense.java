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

package org.apache.ignite.internal.product;

import org.apache.ignite.internal.*;

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Ignite license descriptor. Ignite license is available for
 * information purposes and is checked automatically by Ignite software.
 * License descriptor can be obtains by calling {@link GridProduct#license()} method.
 * @see GridProduct#license()
 */
public interface IgniteProductLicense extends Serializable {
    /**
     * Gets a comma separated list of disabled subsystems.
     *
     * @return Comma separated list of disabled subsystems or {@code null}.
     */
    public String disabledSubsystems();

    /**
     * Gets license version.
     *
     * @return License version.
     */
    public String version();

    /**
     * Gets license ID.
     *
     * @return License ID.
     */
    public UUID id();

    /**
     * Version regular expression.
     *
     * @return Version regular expression.
     */
    public String versionRegexp();

    /**
     * Gets issue date.
     *
     * @return Issue date.
     */
    public Date issueDate();

    /**
     * Gets maintenance time in months. If zero - no restriction.
     *
     * @return Maintenance time.
     */
    public int maintenanceTime();

    /**
     * Gets issue organization.
     *
     * @return Issue organization.
     */
    public String issueOrganization();

    /**
     * Gets user organization.
     *
     * @return User organization.
     */
    public String userOrganization();

    /**
     * Gets license note. It may include textual description of license limitations such
     * as as "Development Only" or "Load-Testing and Staging Only".
     *
     * @return License note.
     */
    public String licenseNote();

    /**
     * Gets user organization URL.
     *
     * @return User organization URL.
     */
    public String userWww();

    /**
     * Gets user organization e-mail.
     *
     * @return User organization e-mail.
     */
    public String userEmail();

    /**
     * Gets user organization contact name.
     *
     * @return User organization contact name.
     */
    public String userName();

    /**
     * Gets expire date.
     *
     * @return Expire date.
     */
    public Date expireDate();

    /**
     * Gets maximum number of nodes. If zero - no restriction.
     *
     * @return Maximum number of nodes.
     */
    public int maxNodes();

    /**
     * Gets maximum number of physical computers or virtual instances. If zero - no restriction.
     * Note that individual physical computer or virtual instance is determined by number of enabled
     * MACs on each computer or instance.
     *
     * @return Maximum number of computers or virtual instances.
     */
    public int maxComputers();

    /**
     * Gets maximum number of CPUs. If zero - no restriction.
     *
     * @return Maximum number of CPUs.
     */
    public int maxCpus();

    /**
     * Gets maximum up time in minutes. If zero - no restriction.
     *
     * @return Maximum up time in minutes.
     */
    public long maxUpTime();

    /**
     * Gets license violation grace period in minutes. If zero - no grace period.
     *
     * @return License violation grace period in minutes.
     */
    public long gracePeriod();

    /**
     * Gets license attribute name if any. Attributes in license will have to match
     * attributes in the grid node.
     *
     * @return Attribute name.
     */
    @Nullable public String attributeName();

    /**
     * Gets value for the license attribute if any. Attributes in license will have to match
     * attributes in the grid node.
     *
     * @return Attribute value.
     */
    @Nullable public String attributeValue();

    /**
     * Gets a comma separated list of allowed cache distribution modes.
     *
     * @return Allowed cache distribution modes or {@code null}.
     */
    @Nullable public String getCacheDistributionModes();
}
