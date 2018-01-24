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

package org.apache.ignite.internal.processors.bulkload.pipeline;

import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** FIXME SHQ */
public class CsvLineProcessorBlock extends PipelineBlock<String, String[]> {

    private final Pattern fieldDelimiter;
    private final char trimChar;

    public CsvLineProcessorBlock(Pattern fieldDelimiter, char trimChar) {
        super();

        this.fieldDelimiter = fieldDelimiter;
        this.trimChar = trimChar;
    }

    @Override public void accept(String input, boolean isEof) throws IgniteCheckedException {
        String[] output = fieldDelimiter.split(input);

        for (int i = 0; i < output.length; i++)
            output[i] = trim(output[i]);

        nextBlock.accept(output, isEof);
    }

    @NotNull private String trim(String fld) {
        int startPos = fld.charAt(0) == trimChar ? 1 : 0;
        int endPos = fld.charAt(fld.length() - 1) == trimChar ? fld.length() - 1 : fld.length();

        return fld.substring(startPos, endPos);
    }
}
