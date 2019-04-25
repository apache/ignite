/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * SPIs that have this annotation present will be checked for consistency within grid.
 * If SPIs are not consistent, then warning will be printed out to the log.
 * <p>
 * Note that SPI consistency courtesy log can also be disabled by disabling
 * {@link org.apache.ignite.configuration.IgniteConfiguration#COURTESY_LOGGER_NAME} category in log configuration.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface IgniteSpiConsistencyChecked {
    /**
     * Optional consistency check means that check will be performed only if
     * SPI class names and versions match.
     */
    public boolean optional();

    /**
     * If false, skip consistency checks for client cluster nodes. Could be useful
     * for SwapSpaceSpi for example, since client nodes has no data at all, so they
     * don't need to be consistent with server nodes.
     */
    public boolean checkClient() default true;
}