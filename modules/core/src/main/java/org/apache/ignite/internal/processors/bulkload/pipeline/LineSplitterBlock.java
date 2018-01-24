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

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** FIXME SHQ */
public class LineSplitterBlock extends PipelineBlock<char[], String> {

    private final Pattern delimiter;

    private StringBuilder leftover = new StringBuilder();

    public LineSplitterBlock(Pattern delimiter) {
        super();

        this.delimiter = delimiter;
    }

    @Override public void accept(char[] chars, boolean isEof) throws IgniteCheckedException {
        leftover.append(chars);

        String input = leftover.toString();
        Matcher matcher = delimiter.matcher(input);

        int lastPos = 0;
        while (matcher.find()) {
            String outStr = input.substring(lastPos, matcher.start());

            nextBlock.accept(outStr, false);

            lastPos = matcher.end();
        }

        if (lastPos != 0)
            leftover.delete(0, lastPos);

        if (isEof && leftover.length() > 0) {
            nextBlock.accept(leftover.toString(), true);
            leftover.setLength(0);
        }
    }
}
