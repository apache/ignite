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

package org.apache.ignite.tensorflow.submitter.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;

/**
 * General command parser that parses arguments and decides which specific command parser should be used.
 */
public class GeneralCommandParser implements CommandParser {
    /**
     * Map with specific command parsers.
     */
    private final Map<String, Function<Supplier<Ignite>, CommandParser>> parsers;

    /**
     * Constructs a new instance of general command parser.
     */
    public GeneralCommandParser() {
        parsers = new HashMap<>();
        parsers.put("start", StartCommandParser::new);
        parsers.put("stop", StopCommandParser::new);
        parsers.put("ps", PsCommandParser::new);
        parsers.put("attach", AttachCommandParser::new);
    }

    /** {@inheritDoc} */
    @Override public Runnable parse(String[] args) {
        if (args.length > 0) {
            Supplier<Ignite> igniteSupplier;
            Function<Supplier<Ignite>, CommandParser> parser;

            if (args.length > 2 && "-c".equals(args[0])) {
                igniteSupplier = getCustomIgniteSupplier(args[1]);
                parser = parsers.get(args[2]);

                if (parser != null)
                    return parser.apply(igniteSupplier).parse(Arrays.copyOfRange(args, 3, args.length));
            }
            else {
                igniteSupplier = getDefaultIgniteSupplier();
                parser = parsers.get(args[0]);

                if (parser != null)
                    return parser.apply(igniteSupplier).parse(Arrays.copyOfRange(args, 1, args.length));
            }
        }

        return null;
    }

    /**
     * Returns Ignite supplier that uses specified configuration.
     *
     * @param cfg Path to configuration.
     * @return Ignite supplier.
     */
    private Supplier<Ignite> getCustomIgniteSupplier(String cfg) {
        return () -> Ignition.start(cfg);
    }

    /**
     * Returns default Ignite supplier.
     *
     * @return Ignite supplier.
     */
    private Supplier<Ignite> getDefaultIgniteSupplier() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setGridLogger(new Slf4jLogger());
        cfg.setClientMode(true);

        return () -> Ignition.start(cfg);
    }
}
