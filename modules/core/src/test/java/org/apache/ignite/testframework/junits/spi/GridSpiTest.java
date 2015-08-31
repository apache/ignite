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

package org.apache.ignite.testframework.junits.spi;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/**
 * Annotates all tests in SPI test framework. Provides implementation class of the SPI and
 * optional dependencies.
 */
@SuppressWarnings({"JavaDoc"})
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface GridSpiTest {
    /**
     * Mandatory implementation class for SPI.
     */
    public Class<? extends IgniteSpi> spi();

    /**
     * Flag indicating whether SPI should be automatically started.
     */
    public boolean trigger() default true;

    /**
     * Flag indicating whether discovery SPI should be automatically started.
     */
    public boolean triggerDiscovery() default false;

    /**
     * Optional discovery SPI property to specify which SPI to use for discovering other nodes.
     * This property is ignored if the spi being tested is an implementation of {@link org.apache.ignite.spi.discovery.DiscoverySpi} or
     * {@link #triggerDiscovery()} is set to {@code false}.
     */
    public Class<? extends DiscoverySpi> discoverySpi() default TcpDiscoverySpi.class;

    /**
     * Optional group this test belongs to.
     */
    public String group() default "";
}