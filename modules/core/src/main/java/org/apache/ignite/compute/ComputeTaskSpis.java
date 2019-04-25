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

package org.apache.ignite.compute;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation allows task to specify what SPIs it wants to use.
 * Starting with {@code Ignite 2.1} you can start multiple instances
 * of {@link org.apache.ignite.spi.loadbalancing.LoadBalancingSpi},
 * {@link org.apache.ignite.spi.failover.FailoverSpi}, and {@link org.apache.ignite.spi.checkpoint.CheckpointSpi}. If you do that,
 * you need to tell a task which SPI to use (by default it will use the fist
 * SPI in the list).
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ComputeTaskSpis {
    /**
     * Optional load balancing SPI name. By default, SPI name is equal
     * to the name of the SPI class. You can change SPI name by explicitly
     * supplying {@link org.apache.ignite.spi.IgniteSpi#getName()} parameter in grid configuration.
     */
    public String loadBalancingSpi() default "";

    /**
     * Optional failover SPI name. By default, SPI name is equal
     * to the name of the SPI class. You can change SPI name by explicitly
     * supplying {@link org.apache.ignite.spi.IgniteSpi#getName()} parameter in grid configuration.
     */
    public String failoverSpi() default "";

    /**
     * Optional checkpoint SPI name. By default, SPI name is equal
     * to the name of the SPI class. You can change SPI name by explicitly
     * supplying {@link org.apache.ignite.spi.IgniteSpi#getName()} parameter in grid configuration.
     */
    public String checkpointSpi() default "";
}
