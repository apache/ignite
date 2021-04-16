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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

/**
 * Iterator over command arguments.
 */
public class CommandArgIterator {
    /** */
    private Iterator<String> argsIt;

    /** */
    private String peekedArg;

    /**
     * Set of common arguments names and high level command name set.
     */
    private final Set<String> commonArgumentsAndHighLevelCommandSet;

    /**
     * @param argsIt Raw argument iterator.
     * @param commonArgumentsAndHighLevelCommandSet All known subcomands.
     */
    public CommandArgIterator(Iterator<String> argsIt, Set<String> commonArgumentsAndHighLevelCommandSet) {
        this.argsIt = argsIt;
        this.commonArgumentsAndHighLevelCommandSet = commonArgumentsAndHighLevelCommandSet;
    }

    /**
     * @return Returns {@code true} if the iteration has more elements.
     */
    public boolean hasNextArg() {
        return peekedArg != null || argsIt.hasNext();
    }

    /**
     * @return <code>true</code> if there's next argument for subcommand.
     */
    public boolean hasNextSubArg() {
        return hasNextArg() && CommandList.of(peekNextArg()) == null &&
            !commonArgumentsAndHighLevelCommandSet.contains(peekNextArg());
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

    /**
     * @return Numeric value.
     */
    public long nextNonNegativeLongArg(String argName) {
        long val = nextLongArg(argName);

        if (val < 0)
            throw new IllegalArgumentException("Invalid value for " + argName + ": " + val);

        return val;
    }

    /**
     * @return Numeric value.
     */
    public int nextNonNegativeIntArg(String argName) {
        int val = nextIntArg(argName);

        if (val < 0)
            throw new IllegalArgumentException("Invalid value for " + argName + ": " + val);

        return val;
    }

    /**
     * @return Numeric value.
     */
    public long nextLongArg(String argName) {
        String str = nextArg("Expecting " + argName);

        try {
            return str.startsWith("0x") ? Long.parseLong(str.substring(2), 16) : Long.parseLong(str);
        }
        catch (NumberFormatException ignored) {
            throw new IllegalArgumentException("Invalid value for " + argName + ": " + str);
        }
    }

    /**
     * @return Numeric value.
     */
    public int nextIntArg(String argName) {
        String str = nextArg("Expecting " + argName);

        try {
            return str.startsWith("0x") ? Integer.parseInt(str.substring(2), 16) : Integer.parseInt(str);
        }
        catch (NumberFormatException ignored) {
            throw new IllegalArgumentException("Invalid value for " + argName + ": " + str);
        }
    }

    /**
     * @param argName Name of argument.
     */
    public Set<String> nextStringSet(String argName) {
        String nextArg = peekNextArg();

        if (isCommandOrOption(nextArg))
            return Collections.emptySet();

        nextArg = nextArg("Expected " + argName);

        return parseStringSet(nextArg);
    }

    /**
     *
     * @param string To scan on for string set.
     * @return Set of string parsed from string param.
     */
    @NotNull public Set<String> parseStringSet(String string) {
        Set<String> namesSet = new HashSet<>();

        for (String name : string.split(",")) {
            if (F.isEmpty(name))
                throw new IllegalArgumentException("Non-empty string expected.");

            namesSet.add(name.trim());
        }
        return namesSet;
    }

    /**
     * Check if raw arg is command or option.
     *
     * @return {@code true} If raw arg is command, overwise {@code false}.
     */
    public static boolean isCommandOrOption(String raw) {
        return raw != null && raw.contains("--");
    }
}
