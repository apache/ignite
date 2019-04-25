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

package org.apache.ignite.tensorflow.submitter.command;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import picocli.CommandLine;

/**
 * Abstract command that contains options common for all commands.
 */
public abstract class AbstractCommand implements Runnable {
    /** Ignite node configuration path. */
    @CommandLine.Option(names = { "-c", "--config" }, description = "Apache Ignite client configuration.")
    protected String cfg;

    /**
     * Returns Ignite instance based on configuration specified in {@link #cfg} field.
     *
     * @return Ignite instance.
     */
    protected Ignite getIgnite() {
        if (cfg != null)
            return Ignition.start(cfg);
        else {
            IgniteConfiguration igniteCfg = new IgniteConfiguration();
            igniteCfg.setGridLogger(new Slf4jLogger());
            igniteCfg.setClientMode(true);

            return Ignition.start(igniteCfg);
        }
    }

    /** */
    public void setCfg(String cfg) {
        this.cfg = cfg;
    }
}
