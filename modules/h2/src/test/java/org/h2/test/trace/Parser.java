/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 */
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
package org.h2.test.trace;

import java.math.BigDecimal;
import java.util.ArrayList;

import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * Parses an entry in a Java-style log file.
 */
class Parser {
    private static final int STRING = 0, NAME = 1, NUMBER = 2, SPECIAL = 3;
    private final Player player;
    private Statement stat;
    private final String line;
    private String token;
    private int tokenType;
    private int pos;

    private Parser(Player player, String line) {
        this.player = player;
        this.line = line;
        read();
    }

    /**
     * Parse a Java statement.
     *
     * @param player the player
     * @param line the statement text
     * @return the statement
     */
    static Statement parseStatement(Player player, String line) {
        Parser p = new Parser(player, line);
        p.parseStatement();
        return p.stat;
    }

    private Statement parseStatement() {
        stat = new Statement(player);
        String name = readToken();
        Object o = player.getObject(name);
        if (o == null) {
            if (readIf(".")) {
                // example: java.lang.System.exit(0);
                parseStaticCall(name);
            } else {
                // example: Statement s1 = ...
                stat.setAssign(name, readToken());
                read("=");
                name = readToken();
                o = player.getObject(name);
                if (o != null) {
                    // example: ... = s1.executeQuery();
                    read(".");
                    parseCall(name, o, readToken());
                } else if (readIf(".")) {
                    // ... = x.y.z("...");
                    parseStaticCall(name);
                }
            }
        } else {
            // example: s1.execute()
            read(".");
            String methodName = readToken();
            parseCall(name, o, methodName);
        }
        return stat;
    }

    private void read() {
        while (line.charAt(pos) == ' ') {
            pos++;
        }
        int start = pos;
        char ch = line.charAt(pos);
        switch (ch) {
        case '\"':
            tokenType = STRING;
            pos++;
            while (pos < line.length()) {
                ch = line.charAt(pos);
                if (ch == '\\') {
                    pos += 2;
                } else if (ch == '\"') {
                    pos++;
                    break;
                } else {
                    pos++;
                }
            }
            break;
        case '.':
        case ',':
        case '(':
        case ')':
        case ';':
        case '{':
        case '}':
        case '[':
        case ']':
        case '=':
            tokenType = SPECIAL;
            pos++;
            break;
        default:
            if (Character.isLetter(ch) || ch == '_') {
                tokenType = NAME;
                pos++;
                while (true) {
                    ch = line.charAt(pos);
                    if (Character.isLetterOrDigit(ch) || ch == '_') {
                        pos++;
                    } else {
                        break;
                    }
                }
            } else if (ch == '-' || Character.isDigit(ch)) {
                tokenType = NUMBER;
                pos++;
                while (true) {
                    ch = line.charAt(pos);
                    if (Character.isDigit(ch)
                            || ".+-eElLxabcdefABCDEF".indexOf(ch) >= 0) {
                        pos++;
                    } else {
                        break;
                    }
                }
            }
        }
        token = line.substring(start, pos);
    }

    private boolean readIf(String s) {
        if (token.equals(s)) {
            read();
            return true;
        }
        return false;
    }

    private String readToken() {
        String s = token;
        read();
        return s;
    }

    private void read(String s) {
        if (!readIf(s)) {
            throw new RuntimeException("Expected: " + s + " got: " + token + " in "
                    + line);
        }
    }

    private Arg parseValue() {
        if (tokenType == STRING) {
            String s = readToken();
            s = StringUtils.javaDecode(s.substring(1, s.length() - 1));
            return new Arg(String.class, s);
        } else if (tokenType == NUMBER) {
            String number = readToken().toLowerCase();
            if (number.endsWith("f")) {
                Float v = Float.parseFloat(number);
                return new Arg(float.class, v);
            } else if (number.endsWith("d") ||
                    number.indexOf('e') >= 0 ||
                    number.indexOf('.') >= 0) {
                Double v = Double.parseDouble(number);
                return new Arg(double.class, v);
            } else if (number.endsWith("L") || number.endsWith("l")) {
                Long v = Long.parseLong(number.substring(0, number.length() - 1));
                return new Arg(long.class, v);
            } else {
                Integer v = Integer.parseInt(number);
                return new Arg(int.class, v);
            }
        } else if (tokenType == NAME) {
            if (readIf("true")) {
                return new Arg(boolean.class, Boolean.TRUE);
            } else if (readIf("false")) {
                return new Arg(boolean.class, Boolean.FALSE);
            } else if (readIf("null")) {
                throw new RuntimeException(
                        "Null: class not specified. Example: (java.lang.String)null");
            } else if (readIf("new")) {
                if (readIf("String")) {
                    read("[");
                    read("]");
                    read("{");
                    ArrayList<Object> values = New.arrayList();
                    do {
                        values.add(parseValue().getValue());
                    } while (readIf(","));
                    read("}");
                    String[] list = values.toArray(new String[0]);
                    return new Arg(String[].class, list);
                } else if (readIf("BigDecimal")) {
                    read("(");
                    BigDecimal value = new BigDecimal((String) parseValue().getValue());
                    read(")");
                    return new Arg(BigDecimal.class, value);
                } else {
                    throw new RuntimeException("Unsupported constructor: " + readToken());
                }
            }
            String name = readToken();
            Object obj = player.getObject(name);
            if (obj != null) {
                return new Arg(obj.getClass(), obj);
            }
            read(".");
            Statement outer = stat;
            stat = new Statement(player);
            parseStaticCall(name);
            Arg s = new Arg(stat);
            stat = outer;
            return s;
        } else if (readIf("(")) {
            read("short");
            read(")");
            String number = readToken();
            return new Arg(short.class, Short.parseShort(number));
        } else {
            throw new RuntimeException("Value expected, got: " + readToken() + " in "
                    + line);
        }
    }

    private void parseCall(String objectName, Object o, String methodName) {
        stat.setMethodCall(objectName, o, methodName);
        ArrayList<Arg> args = New.arrayList();
        read("(");
        while (true) {
            if (readIf(")")) {
                break;
            }
            Arg p = parseValue();
            args.add(p);
            if (readIf(")")) {
                break;
            }
            read(",");
        }
        stat.setArgs(args);
    }

    private void parseStaticCall(String clazz) {
        String last = readToken();
        while (readIf(".")) {
            clazz += last == null ? "" : "." + last;
            last = readToken();
        }
        String methodName = last;
        stat.setStaticCall(clazz);
        parseCall(null, null, methodName);
    }

}
