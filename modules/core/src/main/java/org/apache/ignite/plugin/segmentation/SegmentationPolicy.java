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

package org.apache.ignite.plugin.segmentation;

import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Policy that defines how node will react on topology segmentation. Note that default
 * segmentation policy is defined by {@link IgniteConfiguration#DFLT_SEG_PLC} property.
 * @see SegmentationResolver
 */
public enum SegmentationPolicy {
    /**
     * When segmentation policy is {@code RESTART_JVM}, all listeners will receive
     * {@link org.apache.ignite.events.EventType#EVT_NODE_SEGMENTED} event and then JVM will be restarted.
     * Note, that this will work <b>only</b> if Ignite is started with {@link org.apache.ignite.startup.cmdline.CommandLineStartup}
     * via standard {@code ignite.{sh|bat}} shell script.
     */
    RESTART_JVM,

    /**
     * When segmentation policy is {@code STOP}, all listeners will receive
     * {@link org.apache.ignite.events.EventType#EVT_NODE_SEGMENTED} event and then particular grid node
     * will be stopped via call to {@link org.apache.ignite.Ignition#stop(String, boolean)}.
     */
    STOP,

    /**
     * When segmentation policy is {@code NOOP}, all listeners will receive
     * {@link org.apache.ignite.events.EventType#EVT_NODE_SEGMENTED} event and it is up to user to
     * implement logic to handle this event.
     */
    NOOP
}