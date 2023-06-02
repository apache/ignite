/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.ignite.internal.commandline;

import java.util.Iterator;
import org.apache.ignite.internal.util.lang.PeekableIterator;

/**
 * Iterator over command arguments.
 */
public class CommandArgIterator {
    /** */
    private final PeekableIterator<String> argsIt;

    /**
     * @param argsIt Raw argument iterator.
     */
    public CommandArgIterator(Iterator<String> argsIt) {
        this.argsIt = new PeekableIterator<>(argsIt);
    }

    /**
     * @return Returns {@code true} if the iteration has more elements.
     */
    public boolean hasNextArg() {
        return argsIt.hasNext();
    }

    /**
     * Extract next argument.
     *
     * @param err Error message.
     * @return Next argument value.
     */
    public String nextArg(String err) {
        if (argsIt.hasNext())
            return argsIt.next();

        throw new IllegalArgumentException(err);
    }

    /**
     * Returns the next argument in the iteration, without advancing the iteration.
     *
     * @return Next argument value or {@code null} if no next argument.
     */
    public String peekNextArg() {
        return argsIt.peek();
    }

    /** */
    public PeekableIterator<String> raw() {
        return argsIt;
    }
}
