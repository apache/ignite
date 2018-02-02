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

package org.apache.ignite.internal.processors.bulkload;

import org.apache.ignite.IgniteCheckedException;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

/** A superclass and a factory for bulk load format options. */
public abstract class BulkLoadFormat {
    /** The default input charset. */
    public static final Charset DEFAULT_INPUT_CHARSET = Charset.forName("UTF-8");

    /** The default input charset. */
    public static final Charset DEFAULT_INPUT_CHARSET = Charset.forName("UTF-8");

    /**
     * Returns the format name.
     *
     * @return The format name.
     */
    public abstract String name();

    /**
     * Creates a format options object for a given format name.
     *
     * @param name The name of the format
     * @return The format options object.
     * @throws IgniteCheckedException if the name is not recognized.
     */
    public static BulkLoadFormat createFormatFor(String name) throws IgniteCheckedException {
        switch (name.trim().toUpperCase()) {
            case BulkLoadCsvFormat.NAME:
                return new BulkLoadCsvFormat();

            default:
                throw new IgniteCheckedException("Unknown format name: " + name);
        }
    }

    /**
     * Returns a list of all supported format names.
     *
     * @return The list of all supported format names.
     */
    public static List<String> formatNames() {
        return Arrays.asList(BulkLoadCsvFormat.NAME);
    }
}
