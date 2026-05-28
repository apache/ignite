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

package org.apache.ignite.internal.processors.rollingupgrade.feature;

import org.apache.ignite.internal.processors.rollingupgrade.RollingUpgradeProcessor;

/**
 * Declares the {@link IgniteFeature}s supported by the current Ignite version.
 * The complete set of declared {@link IgniteFeature}s defines the behavior of a node running the current version
 * and determines whether a Rolling Upgrade is supported between the current Ignite version and older Ignite versions.
 *
 * <p><b>Developer Guide</b></p>
 *
 * <p>Developers should introduce a new {@link IgniteFeature} whenever a code change may break backward compatibility
 * with older Ignite versions.</p>
 *
 * <p>Typical cases include:</p>
 *
 * <ol>
 *     <li>Message schema changes (adding, replacing, or removing fields).</li>
 *     <li>Changes to message production or processing logic.</li>
 *     <li>Introduction of new message types and their associated processing logic.</li>
 *     <li>API deprecation or removal.</li>
 *     <li>Changes to persistence logic or on-disk data formats.</li>
 * </ol>
 *
 * <p>Template for Rolling Upgrade-compatible code:</p>
 *
 * <pre>{@code
 * if (ctx.rollingUpgrade().features().isActive(IGNITE_FEATURE)) {
 *     // <new logic>
 * }
 * else {
 *     // <old logic>
 * }
 * }</pre>
 *
 * <p>{@link IgniteFeature} ID and naming conventions:</p>
 *
 * <ol>
 *     <li>Each variable declaring an {@link IgniteFeature} must end with the {@code _FEATURE} suffix.</li>
 *     <li>IDs of existing {@link IgniteFeature}s must not be modified.</li>
 *     <li>IDs assigned to new {@link IgniteFeature}s must increase monotonically.</li>
 *     <li>{@link IgniteFeature}s must be declared in ascending order of their identifiers.</li>
 * </ol>
 *
 * <p><b>Release Manager Guide</b></p>
 * <p><b>Release based on the Ignite main branch</b></p>
 * <p>The release manager should:</p>
 *
 * <ol>
 *     <li>
 *         Determine which previous Ignite versions should remain eligible for Rolling Upgrade to the current version.
 *     </li>
 *     <li>
 *         Remove {@link IgniteFeature}s introduced in Ignite versions from which Rolling Upgrade is no longer supported.
 *         Such removals must always form a contiguous range starting from the lowest {@link IgniteFeature} identifier.
 *     </li>
 *     <li>
 *         Remove code that emulates the behavior of Ignite versions from which Rolling Upgrade is no longer supported.
 *         As a result, only the {@code <new logic>} branch should remain.
 *     </li>
 * </ol>
 *
 * <p><b>Release based on a legacy branch (backporting features to earlier Ignite versions)</b></p>
 * <p>The release manager must follow the rules listed below:</p>
 * <ol>
 *     <li>Identifiers of backported {@link IgniteFeature}s must not be modified.</li>
 *     <li>Removal of existing {@link IgniteFeature}s is strictly prohibited.</li>
 *     <li>Introducing new {@link IgniteFeature}s that do not exist in the main branch is strictly prohibited.</li>
 * </ol>
 *
 * @see IgniteFeature
 * @see IgniteFeatureSet
 * @see IgniteFeatureManager
 * @see RollingUpgradeProcessor
 */
public class IgniteReleaseFeatures {
    /** */
    public static final IgniteFeature ROLLING_UPGRADE_FEATURE = new IgniteCoreFeature(0);
}
