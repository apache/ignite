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
import java.util.Set;

/**
 *
 */
class CommandArgIterator {
    /** */
    private Iterator<String> argsIt;

    /** */
    private String peekedArg;

    public CommandArgIterator(Iterator<String> argsIt) {
        this.argsIt = argsIt;
    }

    /**
     * @return Returns {@code true} if the iteration has more elements.
     */
    public boolean hasNextArg() {
        return peekedArg != null || argsIt.hasNext();
    }

    /**
     * @param subCommandsSet All known subcomands.
     * @return <code>true</code> if there's next argument for subcommand.
     */
    public boolean hasNextSubArg(Set<String> subCommandsSet) {
        return hasNextArg() && Commands.of(peekNextArg()) == null && !subCommandsSet.contains(peekNextArg());
    }

    /**
     * Extract next argument.
     *
     * @param err Error message.
     * @return Next argument value.
     */
    public String nextArg(String err) {
        if (peekedArg != null) {
            String res = peekedArg;

            peekedArg = null;

            return res;
        }

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
        if (peekedArg == null && argsIt.hasNext())
            peekedArg = argsIt.next();

        return peekedArg;
    }
}
