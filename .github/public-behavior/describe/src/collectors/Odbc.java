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
package collectors;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import framework.BuiltTree;
import framework.Collector;
import framework.Declarations;

/**
 * Describes the attributes of the ODBC connection string with the type and default of each. The ODBC driver is written in C++
 * and declares them as constants: an attribute in {@code ConnectionStringParser::Key} and its default in
 * {@code Configuration::DefaultValue} under the same name. The constants are read from the sources, so no C++ build is needed.
 */
public class Odbc implements Collector {
    /** Source with the attribute names. */
    private static final String KEYS = "modules/platforms/cpp/odbc/src/config/connection_string_parser.cpp";

    /** Source with the defaults. */
    private static final String DEFAULTS = "modules/platforms/cpp/odbc/src/config/configuration.cpp";

    /** Definition of an attribute name: name of the constant and the attribute. */
    private static final Pattern KEY = Pattern.compile("ConnectionStringParser::Key::(\\w+)\\s*=\\s*\"([^\"]*)\"\\s*;");

    /** Definition of a default: type, name of the constant and the value as the source writes it. */
    private static final Pattern DEFAULT = Pattern.compile("const\\s+([\\w:]+)&?\\s+Configuration::DefaultValue::(\\w+)\\s*=\\s*([^;]+);");

    /** {@inheritDoc} */
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        Map<String, String> defaults = new HashMap<>();

        for (String line : Files.readAllLines(tree.resolve(DEFAULTS))) {
            Matcher m = DEFAULT.matcher(line);

            if (m.find())
                defaults.put(m.group(2), m.group(1) + ", default " + m.group(3).trim());
        }

        List<String> lines = Files.readAllLines(tree.resolve(KEYS));
        int found = 0;

        for (int i = 0; i < lines.size(); i++) {
            Matcher m = KEY.matcher(lines.get(i));

            if (!m.find())
                continue;

            out.add("Configuration", "ODBC connection attribute " + m.group(2), defaults.getOrDefault(m.group(1), "no default"), "",
                KEYS + ':' + (i + 1));

            found++;
        }

        if (found == 0)
            out.error("No ODBC connection attributes found in " + KEYS);

        if (defaults.isEmpty())
            out.error("No ODBC defaults found in " + DEFAULTS);
    }
}
