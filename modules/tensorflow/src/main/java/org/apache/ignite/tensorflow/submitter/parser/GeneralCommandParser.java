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
import org.apache.ignite.tensorflow.submitter.command.Command;

/**
 * General command parser that parses arguments and decides which specific command parser should be used.
 */
public class GeneralCommandParser implements CommandParser {
    /**
     * Map with specific command parsers.
     */
    private final Map<String, CommandParser> parsers;

    /**
     * Constructs a new instance of general command parser.
     */
    public GeneralCommandParser() {
        parsers = new HashMap<>();
        parsers.put("start", new StartCommandParser());
        parsers.put("stop", new StopCommandParser());
        parsers.put("ps", new PsCommandParser());
        parsers.put("describe", new DescribeCommandParser());
    }

    /** {@inheritDoc} */
    @Override public Command parse(String[] args) {
        if (args.length > 0) {
            CommandParser parser = parsers.get(args[0]);
            if (parser != null)
                return parser.parse(Arrays.copyOfRange(args, 1, args.length));
        }

        return null;
    }
}
