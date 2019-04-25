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

package org.apache.ignite.examples.igfs;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

/**
 * Starts up an empty node with IGFS configuration.
 * You can also start a stand-alone Ignite instance by passing the path
 * to configuration file to {@code 'ignite.{sh|bat}'} script, like so:
 * {@code 'ignite.sh examples/config/filesystem/example-igfs.xml'}.
 * <p>
 * The difference is that running this class from IDE adds all example classes to classpath
 * but running from command line doesn't.
 */
public class IgfsNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        Ignition.start("examples/config/filesystem/example-igfs.xml");
    }
}