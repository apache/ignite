/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map.Entry;

import org.h2.command.Parser;
import org.h2.test.TestBase;
import org.h2.util.ParserUtil;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Tests keywords.
 */
public class TestKeywords extends TestBase {

    private enum TokenType {
        IDENTIFIER,

        KEYWORD,

        CONTEXT_SENSITIVE_KEYWORD;
    }

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        final HashMap<String, TokenType> tokens = new HashMap<>();
        ClassReader r = new ClassReader(Parser.class.getResourceAsStream("Parser.class"));
        r.accept(new ClassVisitor(Opcodes.ASM7) {
            @Override
            public FieldVisitor visitField(int access, String name, String descriptor, String signature,
                    Object value) {
                add(value);
                return null;
            }

            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
                    String[] exceptions) {
                return new MethodVisitor(Opcodes.ASM7) {
                    @Override
                    public void visitLdcInsn(Object value) {
                        add(value);
                    }
                };
            }

            void add(Object value) {
                if (!(value instanceof String)) {
                    return;
                }
                String s = (String) value;
                int l = s.length();
                if (l == 0) {
                    return;
                }
                for (int i = 0; i < l; i++) {
                    char ch = s.charAt(i);
                    if ((ch < 'A' || ch > 'Z') && ch != '_') {
                        return;
                    }
                }
                final TokenType type;
                switch (ParserUtil.getSaveTokenType(s, false, 0, l, true)) {
                case ParserUtil.IDENTIFIER:
                    type = TokenType.IDENTIFIER;
                    break;
                case ParserUtil.KEYWORD:
                    type = TokenType.CONTEXT_SENSITIVE_KEYWORD;
                    break;
                default:
                    type = TokenType.KEYWORD;
                }
                tokens.put(s, type);
            }
        }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:keywords")) {
            Statement stat = conn.createStatement();
            for (Entry<String, TokenType> entry : tokens.entrySet()) {
                String s = entry.getKey();
                TokenType type = entry.getValue();
                Throwable exception1 = null, exception2 = null;
                try {
                    stat.execute("CREATE TABLE " + s + '(' + s + " INT)");
                    stat.execute("INSERT INTO " + s + '(' + s + ") VALUES (10)");
                } catch (Throwable t) {
                    exception1 = t;
                }
                if (exception1 == null) {
                    try {
                        try (ResultSet rs = stat.executeQuery("SELECT " + s + " FROM " + s)) {
                            assertTrue(rs.next());
                            assertEquals(10, rs.getInt(1));
                            assertFalse(rs.next());
                        }
                        try (ResultSet rs = stat.executeQuery("SELECT SUM(" + s + ") " + s + " FROM " + s + ' ' + s)) {
                            assertTrue(rs.next());
                            assertEquals(10, rs.getInt(1));
                            assertFalse(rs.next());
                            assertEquals(s, rs.getMetaData().getColumnLabel(1));
                        }
                        stat.execute("DROP TABLE " + s);
                        stat.execute("CREATE TABLE TEST(" + s + " VARCHAR) AS VALUES '-'");
                        String str;
                        try (ResultSet rs = stat.executeQuery("SELECT TRIM(" + s + " FROM '--a--') FROM TEST")) {
                            assertTrue(rs.next());
                            str = rs.getString(1);
                        }
                        stat.execute("DROP TABLE TEST");
                        try (ResultSet rs = stat
                                .executeQuery("SELECT ROW_NUMBER() OVER(" + s + ") WINDOW " + s + " AS ()")) {
                        }
                        if (!"a".equals(str)) {
                            exception2 = new AssertionError();
                        }
                    } catch (Throwable t) {
                        exception2 = t;
                        stat.execute("DROP TABLE IF EXISTS TEST");
                    }
                }
                switch (type) {
                case IDENTIFIER:
                    if (exception1 != null) {
                        throw new AssertionError(s + " must be a keyword.", exception1);
                    }
                    if (exception2 != null) {
                        throw new AssertionError(s + " must be a context-sensitive keyword.", exception2);
                    }
                    break;
                case KEYWORD:
                    if (exception1 == null && exception2 == null) {
                        throw new AssertionError(s + " may be removed from a list of keywords.");
                    }
                    if (exception1 == null) {
                        throw new AssertionError(s + " may be a context-sensitive keyword.");
                    }
                    break;
                case CONTEXT_SENSITIVE_KEYWORD:
                    if (exception1 != null) {
                        throw new AssertionError(s + " must be a keyword.", exception1);
                    }
                    if (exception2 == null) {
                        throw new AssertionError(s + " may be removed from a list of context-sensitive keywords.");
                    }
                    break;
                default:
                    fail();
                }
            }
        }
    }

}
