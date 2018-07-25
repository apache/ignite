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

import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.tensorflow.submitter.command.AttachCommand;

/**
 * Command parser that parses "describe" command.
 */
public class AttachCommandParser implements CommandParser {
    /** Ignite supplier. */
    private final Supplier<Ignite> igniteSupplier;

    /**
     * Constructs a new instance of "attach" command parser.
     *
     * @param igniteSupplier Ignite supplier.
     */
    public AttachCommandParser(Supplier<Ignite> igniteSupplier) {
        this.igniteSupplier = igniteSupplier;
    }

    /** {@inheritDoc} */
    @Override public Runnable parse(String[] args) {
        if (args.length == 1) {
            UUID clusterId = UUID.fromString(args[0]);
            return new AttachCommand(igniteSupplier, clusterId);
        }

        return null;
    }
}
