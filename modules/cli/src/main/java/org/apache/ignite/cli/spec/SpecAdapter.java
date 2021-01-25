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

package org.apache.ignite.cli.spec;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.ignite.cli.VersionProvider;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Model.CommandSpec;

/**
 * Base class for any Ignite commands.
 */
@CommandLine.Command(versionProvider = VersionProvider.class)
public abstract class SpecAdapter implements Runnable {
    private static final String[] BANNER = new String[] {
        "",
        "  @|red,bold          #|@              ___                         __",
        "  @|red,bold        ###|@             /   |   ____   ____ _ _____ / /_   ___",
        "  @|red,bold    #  #####|@           / /| |  / __ \\ / __ `// ___// __ \\ / _ \\",
        "  @|red,bold  ###  ######|@         / ___ | / /_/ // /_/ // /__ / / / // ___/",
        "  @|red,bold #####  #######|@      /_/  |_|/ .___/ \\__,_/ \\___//_/ /_/ \\___/",
        "  @|red,bold #######  ######|@            /_/",
        "  @|red,bold   ########  ####|@        ____               _  __           @|red,bold _____|@",
        "  @|red,bold  #  ########  ##|@       /  _/____ _ ____   (_)/ /_ ___     @|red,bold |__  /|@",
        "  @|red,bold ####  #######  #|@       / / / __ `// __ \\ / // __// _ \\     @|red,bold /_ <|@",
        "  @|red,bold  #####  #####|@        _/ / / /_/ // / / // // /_ / ___/   @|red,bold ___/ /|@",
        "  @|red,bold    ####  ##|@         /___/ \\__, //_/ /_//_/ \\__/ \\___/   @|red,bold /____/|@",
        "  @|red,bold      ##|@                  /____/\n"
    };

    @CommandLine.Spec
    protected CommandSpec spec;

    public String banner() {
        String banner = Arrays
            .stream(BANNER)
            .map(Ansi.AUTO::string)
            .collect(Collectors.joining("\n"));

        return '\n' + banner + '\n' + " ".repeat(22) + spec.version()[0] + "\n\n";
    }
}
