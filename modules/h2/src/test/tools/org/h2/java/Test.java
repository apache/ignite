/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import org.h2.test.TestBase;

/**
 * A test for the Java parser.
 */
public class Test extends TestBase {

    /**
     * Start the task with the given arguments.
     *
     * @param args the arguments, or null
     */
    public static void main(String... args) throws IOException {
        new Test().test();
    }

    @Override
    public void test() throws IOException {
        // g++ -o test test.cpp
        // chmod +x test
        // ./test

        // TODO initialize fields

        // include files:
        // /usr/include/c++/4.2.1/tr1/stdio.h
        // /usr/include/stdio.h
        // inttypes.h

        // not supported yet:
        // exceptions
        // HexadecimalFloatingPointLiteral
        // int x()[] { return null; }
        // import static
        // import *
        // initializer blocks
        // access to static fields with instance variable
        // final variables (within blocks, parameter list)
        // Identifier : (labels)
        // ClassOrInterfaceDeclaration within blocks
        // (or any other nested classes)
        // assert

        assertEquals("\\\\" + "u0000", JavaParser.replaceUnicode("\\\\" + "u0000"));
        assertEquals("\u0000", JavaParser.replaceUnicode("\\" + "u0000"));
        assertEquals("\u0000", JavaParser.replaceUnicode("\\" + "uu0000"));
        assertEquals("\\\\" + "\u0000", JavaParser.replaceUnicode("\\\\\\" + "u0000"));

        assertEquals("0", JavaParser.readNumber("0a"));
        assertEquals("0l", JavaParser.readNumber("0l"));
        assertEquals("0xFFL", JavaParser.readNumber("0xFFLx"));
        assertEquals("0xDadaCafe", JavaParser.readNumber("0xDadaCafex"));
        assertEquals("1.40e-45f", JavaParser.readNumber("1.40e-45fx"));
        assertEquals("1e1f", JavaParser.readNumber("1e1fx"));
        assertEquals("2.f", JavaParser.readNumber("2.fx"));
        assertEquals(".3d", JavaParser.readNumber(".3dx"));
        assertEquals("6.022137e+23f", JavaParser.readNumber("6.022137e+23f+1"));

        JavaParser parser = new JavaParser();
        parser.parse("src/tools/org/h2", "java.lang.Object");
        parser.parse("src/tools/org/h2", "java.lang.String");
        parser.parse("src/tools/org/h2", "java.lang.Math");
        parser.parse("src/tools/org/h2", "java.lang.Integer");
        parser.parse("src/tools/org/h2", "java.lang.Long");
        parser.parse("src/tools/org/h2", "java.lang.StringBuilder");
        parser.parse("src/tools/org/h2", "java.io.PrintStream");
        parser.parse("src/tools/org/h2", "java.lang.System");
        parser.parse("src/tools/org/h2", "java.util.Arrays");
        parser.parse("src/tools", "org.h2.java.TestApp");

        PrintWriter w = new PrintWriter(System.out);
        parser.writeHeader(w);
        parser.writeSource(w);
        w.flush();
        w = new PrintWriter(new FileWriter("bin/test.cpp"));
        parser.writeHeader(w);
        parser.writeSource(w);
        w.close();

    }

}
