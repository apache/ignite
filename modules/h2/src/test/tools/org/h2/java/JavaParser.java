/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import org.h2.util.New;

/**
 * Converts Java to C.
 */
public class JavaParser {

    /**
     * Whether ref-counting is used.
     */
    public static final boolean REF_COUNT = false;

    /**
     * Whether ref-counting is used for constants.
     */
    public static final boolean REF_COUNT_STATIC = false;

    private static final HashMap<String, ClassObj> BUILT_IN_CLASSES = new HashMap<>();

    private static final int TOKEN_LITERAL_CHAR = 0;
    private static final int TOKEN_LITERAL_STRING = 1;
    private static final int TOKEN_LITERAL_NUMBER = 2;
    private static final int TOKEN_RESERVED = 3;
    private static final int TOKEN_IDENTIFIER = 4;
    private static final int TOKEN_OTHER = 5;

    private static final HashSet<String> RESERVED = new HashSet<>();
    private static final HashMap<String, String> JAVA_IMPORT_MAP = new HashMap<>();

    private final ArrayList<ClassObj> allClasses = New.arrayList();

    private String source;

    private ParseState current = new ParseState();

    private String packageName;
    private ClassObj classObj;
    private int nextClassId;
    private MethodObj method;
    private FieldObj thisPointer;
    private final HashMap<String, String> importMap = new HashMap<>();
    private final HashMap<String, ClassObj> classes = new HashMap<>();
    private final LinkedHashMap<String, FieldObj> localVars =
            new LinkedHashMap<>();
    private final HashMap<String, MethodObj> allMethodsMap = new HashMap<>();
    private final ArrayList<Statement> nativeHeaders = New.arrayList();
    private final HashMap<String, String> stringToStringConstantMap = new HashMap<>();
    private final HashMap<String, String> stringConstantToStringMap = new HashMap<>();

    public JavaParser() {
        addBuiltInTypes();
    }

    private void addBuiltInTypes() {
        String[] list = { "abstract", "continue", "for", "new", "switch",
                "assert", "default", "if", "package", "synchronized",
                "boolean", "do", "goto", "private", "this", "break", "double",
                "implements", "protected", "throw", "byte", "else", "import",
                "public", "throws", "case", "enum", "instanceof", "return",
                "transient", "catch", "extends", "int", "short", "try", "char",
                "final", "interface", "static", "void", "class", "finally",
                "long", "strictfp", "volatile", "const", "float", "native",
                "super", "while", "true", "false", "null" };
        for (String s : list) {
            RESERVED.add(s);
        }
        int id = 0;
        addBuiltInType(id++, true, 0, "void");
        addBuiltInType(id++, true, 1, "boolean");
        addBuiltInType(id++, true, 2, "byte");
        addBuiltInType(id++, true, 3, "short");
        addBuiltInType(id++, true, 4, "char");
        addBuiltInType(id++, true, 5, "int");
        addBuiltInType(id++, true, 6, "long");
        addBuiltInType(id++, true, 7, "float");
        addBuiltInType(id++, true, 8, "double");
        String[] java = { "Boolean", "Byte", "Character", "Class",
                "ClassLoader", "Double", "Float", "Integer", "Long", "Math",
                "Number", "Object", "Runtime", "Short", "String",
                "StringBuffer", "StringBuilder", "System", "Thread",
                "ThreadGroup", "ThreadLocal", "Throwable", "Void" };
        for (String s : java) {
            JAVA_IMPORT_MAP.put(s, "java.lang." + s);
            addBuiltInType(id++, false, 0, "java.lang." + s);
        }
        nextClassId = id;
    }

    /**
     * Get the wrapper class for the given primitive class.
     *
     * @param c the class
     * @return the wrapper class
     */
    ClassObj getWrapper(ClassObj c) {
        switch (c.id) {
        case 1:
            return getClass("java.lang.Boolean");
        case 2:
            return getClass("java.lang.Byte");
        case 3:
            return getClass("java.lang.Short");
        case 4:
            return getClass("java.lang.Character");
        case 5:
            return getClass("java.lang.Integer");
        case 6:
            return getClass("java.lang.Long");
        case 7:
            return getClass("java.lang.Float");
        case 8:
            return getClass("java.lang.Double");
        }
        throw new RuntimeException("not a primitive type: " + classObj);
    }

    private void addBuiltInType(int id, boolean primitive, int primitiveType,
            String type) {
        ClassObj c = new ClassObj();
        c.id = id;
        c.className = type;
        c.isPrimitive = primitive;
        c.primitiveType = primitiveType;
        BUILT_IN_CLASSES.put(type, c);
        addClass(c);
    }

    private void addClass(ClassObj c) {
        int id = c.id;
        while (id >= allClasses.size()) {
            allClasses.add(null);
        }
        allClasses.set(id, c);
    }

    /**
     * Parse the source code.
     *
     * @param baseDir the base directory
     * @param className the fully qualified name of the class to parse
     */
    void parse(String baseDir, String className) {
        String fileName = baseDir + "/" + className.replace('.', '/') + ".java";
        current = new ParseState();
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, "r");
            byte[] buff = new byte[(int) file.length()];
            file.readFully(buff);
            source = new String(buff, StandardCharsets.UTF_8);
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        source = replaceUnicode(source);
        source = removeRemarks(source);
        try {
            readToken();
            parseCompilationUnit();
        } catch (Exception e) {
            throw new RuntimeException(source.substring(0, current.index)
                    + "[*]" + source.substring(current.index), e);
        }
    }

    private static String cleanPackageName(String name) {
        if (name.startsWith("org.h2.java.lang")
                || name.startsWith("org.h2.java.io")) {
            return name.substring("org.h2.".length());
        }
        return name;
    }

    private void parseCompilationUnit() {
        if (readIf("package")) {
            packageName = cleanPackageName(readQualifiedIdentifier());
            read(";");
        }
        while (readIf("import")) {
            String importPackageName = cleanPackageName(readQualifiedIdentifier());
            String importClass = importPackageName.substring(importPackageName
                    .lastIndexOf('.') + 1);
            importMap.put(importClass, importPackageName);
            read(";");
        }
        while (true) {
            Statement s = readNativeStatementIf();
            if (s == null) {
                break;
            }
            nativeHeaders.add(s);
        }
        while (true) {
            boolean isPublic = readIf("public");
            boolean isInterface;
            if (readIf("class")) {
                isInterface = false;
            } else {
                read("interface");
                isInterface = true;
            }
            String name = readIdentifier();
            classObj = BUILT_IN_CLASSES.get(packageName + "." + name);
            if (classObj == null) {
                classObj = new ClassObj();
                classObj.id = nextClassId++;
            }
            classObj.isPublic = isPublic;
            classObj.isInterface = isInterface;
            classObj.className = packageName == null ? "" : (packageName + ".")
                    + name;
            // import this class
            importMap.put(name, classObj.className);
            addClass(classObj);
            classes.put(classObj.className, classObj);
            if (readIf("extends")) {
                classObj.superClassName = readQualifiedIdentifier();
            }
            if (readIf("implements")) {
                while (true) {
                    classObj.interfaceNames.add(readQualifiedIdentifier());
                    if (!readIf(",")) {
                        break;
                    }
                }
            }
            parseClassBody();
            if (current.token == null) {
                break;
            }
        }
    }

    private boolean isTypeOrIdentifier() {
        if (BUILT_IN_CLASSES.containsKey(current.token)) {
            return true;
        }
        return current.type == TOKEN_IDENTIFIER;
    }

    private ClassObj getClass(String type) {
        ClassObj c = getClassIf(type);
        if (c == null) {
            throw new RuntimeException("Unknown type: " + type);
        }
        return c;
    }

    /**
     * Get the class for a built-in type.
     *
     * @param type the type
     * @return the class or null if not found
     */
    static ClassObj getBuiltInClass(String type) {
        return BUILT_IN_CLASSES.get(type);
    }

    private ClassObj getClassIf(String type) {
        ClassObj c = BUILT_IN_CLASSES.get(type);
        if (c != null) {
            return c;
        }
        c = classes.get(type);
        if (c != null) {
            return c;
        }
        String mappedType = importMap.get(type);
        if (mappedType == null) {
            mappedType = JAVA_IMPORT_MAP.get(type);
            if (mappedType == null) {
                return null;
            }
        }
        c = classes.get(mappedType);
        if (c == null) {
            c = BUILT_IN_CLASSES.get(mappedType);
            if (c == null) {
                throw new RuntimeException("Unknown class: " + mappedType);
            }
        }
        return c;
    }

    private void parseClassBody() {
        read("{");
        localVars.clear();
        while (true) {
            if (readIf("}")) {
                break;
            }
            thisPointer = null;
            while (true) {
                Statement s = readNativeStatementIf();
                if (s == null) {
                    break;
                }
                classObj.nativeCode.add(s);
            }
            thisPointer = null;
            HashSet<String> annotations = new HashSet<>();
            while (readIf("@")) {
                String annotation = readIdentifier();
                annotations.add(annotation);
            }
            boolean isIgnore = annotations.contains("Ignore");
            boolean isLocalField = annotations.contains("Local");
            boolean isStatic = false;
            boolean isFinal = false;
            boolean isPrivate = false;
            boolean isPublic = false;
            boolean isNative = false;
            while (true) {
                if (readIf("static")) {
                    isStatic = true;
                } else if (readIf("final")) {
                    isFinal = true;
                } else if (readIf("native")) {
                    isNative = true;
                } else if (readIf("private")) {
                    isPrivate = true;
                } else if (readIf("public")) {
                    isPublic = true;
                } else {
                    break;
                }
            }
            if (readIf("{")) {
                method = new MethodObj();
                method.isIgnore = isIgnore;
                method.name = isStatic ? "cl_init_obj" : "";
                method.isStatic = isStatic;
                localVars.clear();
                if (!isStatic) {
                    initThisPointer();
                }
                method.block = readStatement();
                classObj.addMethod(method);
            } else {
                String typeName = readTypeOrIdentifier();
                Type type = readType(typeName);
                method = new MethodObj();
                method.isIgnore = isIgnore;
                method.returnType = type;
                method.isStatic = isStatic;
                method.isFinal = isFinal;
                method.isPublic = isPublic;
                method.isPrivate = isPrivate;
                method.isNative = isNative;
                localVars.clear();
                if (!isStatic) {
                    initThisPointer();
                }
                if (readIf("(")) {
                    if (type.classObj != classObj) {
                        throw getSyntaxException("Constructor of wrong type: "
                                + type);
                    }
                    method.name = "";
                    method.isConstructor = true;
                    parseFormalParameters(method);
                    if (!readIf(";")) {
                        method.block = readStatement();
                    }
                    classObj.addMethod(method);
                    addMethod(method);
                } else {
                    String name = readIdentifier();
                    if (name.endsWith("Method")) {
                        name = name.substring(0,
                                name.length() - "Method".length());
                    }
                    method.name = name;
                    if (readIf("(")) {
                        parseFormalParameters(method);
                        if (!readIf(";")) {
                            method.block = readStatement();
                        }
                        classObj.addMethod(method);
                        addMethod(method);
                    } else {
                        FieldObj field = new FieldObj();
                        field.isIgnore = isIgnore;
                        field.isLocalField = isLocalField;
                        field.type = type;
                        field.name = name;
                        field.isStatic = isStatic;
                        field.isFinal = isFinal;
                        field.isPublic = isPublic;
                        field.isPrivate = isPrivate;
                        field.declaredClass = classObj;
                        if (readIf("=")) {
                            if (field.type.arrayLevel > 0 && readIf("{")) {
                                field.value = readArrayInit(field.type);
                            } else {
                                field.value = readExpr();
                            }
                        } else {
                            field.value = field.type.getDefaultValue(this);
                        }
                        read(";");
                        if (isStatic) {
                            classObj.addStaticField(field);
                        } else {
                            classObj.addInstanceField(field);
                        }
                    }
                }
            }
        }
    }

    private void addMethod(MethodObj m) {
        if (m.isStatic) {
            return;
        }
        MethodObj old = allMethodsMap.get(m.name);
        if (old != null) {
            old.isVirtual = true;
            m.isVirtual = true;
        } else {
            allMethodsMap.put(m.name, m);
        }
    }

    private Expr readArrayInit(Type type) {
        ArrayInitExpr expr = new ArrayInitExpr();
        expr.type = new Type();
        expr.type.classObj = type.classObj;
        expr.type.arrayLevel = type.arrayLevel - 1;
        if (!readIf("}")) {
            while (true) {
                expr.list.add(readExpr());
                if (readIf("}")) {
                    break;
                }
                read(",");
                if (readIf("}")) {
                    break;
                }
            }
        }
        return expr;
    }

    private void initThisPointer() {
        thisPointer = new FieldObj();
        thisPointer.isVariable = true;
        thisPointer.name = "this";
        thisPointer.type = new Type();
        thisPointer.type.classObj = classObj;
    }

    private Type readType(String name) {
        Type type = new Type();
        type.classObj = getClass(name);
        while (readIf("[")) {
            read("]");
            type.arrayLevel++;
        }
        if (readIf("...")) {
            type.arrayLevel++;
            type.isVarArgs = true;
        }
        return type;
    }

    private void parseFormalParameters(MethodObj methodObj) {
        if (readIf(")")) {
            return;
        }
        while (true) {
            FieldObj field = new FieldObj();
            field.isVariable = true;
            String typeName = readTypeOrIdentifier();
            field.type = readType(typeName);
            if (field.type.isVarArgs) {
                methodObj.isVarArgs = true;
            }
            field.name = readIdentifier();
            methodObj.parameters.put(field.name, field);
            if (readIf(")")) {
                break;
            }
            read(",");
        }
    }

    private String readTypeOrIdentifier() {
        if (current.type == TOKEN_RESERVED) {
            if (BUILT_IN_CLASSES.containsKey(current.token)) {
                return read();
            }
        }
        String s = readIdentifier();
        while (readIf(".")) {
            s += "." + readIdentifier();
        }
        return s;
    }

    private Statement readNativeStatementIf() {
        if (readIf("//")) {
            boolean isC = readIdentifierIf("c");
            int start = current.index;
            while (source.charAt(current.index) != '\n') {
                current.index++;
            }
            String s = source.substring(start, current.index).trim();
            StatementNative stat = new StatementNative(s);
            read();
            return isC ? stat : null;
        } else if (readIf("/*")) {
            boolean isC = readIdentifierIf("c");
            int start = current.index;
            while (source.charAt(current.index) != '*'
                    || source.charAt(current.index + 1) != '/') {
                current.index++;
            }
            String s = source.substring(start, current.index).trim();
            StatementNative stat = new StatementNative(s);
            current.index += 2;
            read();
            return isC ? stat : null;
        }
        return null;
    }

    private Statement readStatement() {
        Statement s = readNativeStatementIf();
        if (s != null) {
            return s;
        }
        if (readIf(";")) {
            return new EmptyStatement();
        } else if (readIf("{")) {
            StatementBlock stat = new StatementBlock();
            while (true) {
                if (readIf("}")) {
                    break;
                }
                stat.instructions.add(readStatement());
            }
            return stat;
        } else if (readIf("if")) {
            IfStatement ifStat = new IfStatement();
            read("(");
            ifStat.condition = readExpr();
            read(")");
            ifStat.block = readStatement();
            if (readIf("else")) {
                ifStat.elseBlock = readStatement();
            }
            return ifStat;
        } else if (readIf("while")) {
            WhileStatement whileStat = new WhileStatement();
            read("(");
            whileStat.condition = readExpr();
            read(")");
            whileStat.block = readStatement();
            return whileStat;
        } else if (readIf("break")) {
            read(";");
            return new BreakStatement();
        } else if (readIf("continue")) {
            read(";");
            return new ContinueStatement();
        } else if (readIf("switch")) {

            read("(");
            SwitchStatement switchStat = new SwitchStatement(readExpr());
            read(")");
            read("{");
            while (true) {
                if (readIf("default")) {
                    read(":");
                    StatementBlock block = new StatementBlock();
                    switchStat.setDefaultBlock(block);
                    while (true) {
                        block.instructions.add(readStatement());
                        if (current.token.equals("case")
                                || current.token.equals("default")
                                || current.token.equals("}")) {
                            break;
                        }
                    }
                } else if (readIf("case")) {
                    Expr expr = readExpr();
                    read(":");
                    StatementBlock block = new StatementBlock();
                    while (true) {
                        block.instructions.add(readStatement());
                        if (current.token.equals("case")
                                || current.token.equals("default")
                                || current.token.equals("}")) {
                            break;
                        }
                    }
                    switchStat.addCase(expr, block);
                } else if (readIf("}")) {
                    break;
                }
            }
            return switchStat;
        } else if (readIf("for")) {
            ForStatement forStat = new ForStatement();
            read("(");
            ParseState back = copyParseState();
            try {
                String typeName = readTypeOrIdentifier();
                Type type = readType(typeName);
                String name = readIdentifier();
                FieldObj f = new FieldObj();
                f.name = name;
                f.type = type;
                f.isVariable = true;
                localVars.put(name, f);
                read(":");
                forStat.iterableType = type;
                forStat.iterableVariable = name;
                forStat.iterable = readExpr();
            } catch (Exception e) {
                current = back;
                forStat.init = readStatement();
                forStat.condition = readExpr();
                read(";");
                do {
                    forStat.updates.add(readExpr());
                } while (readIf(","));
            }
            read(")");
            forStat.block = readStatement();
            return forStat;
        } else if (readIf("do")) {
            DoWhileStatement doWhileStat = new DoWhileStatement();
            doWhileStat.block = readStatement();
            read("while");
            read("(");
            doWhileStat.condition = readExpr();
            read(")");
            read(";");
            return doWhileStat;
        } else if (readIf("return")) {
            ReturnStatement returnStat = new ReturnStatement();
            if (!readIf(";")) {
                returnStat.expr = readExpr();
                read(";");
            }
            return returnStat;
        } else {
            if (isTypeOrIdentifier()) {
                ParseState start = copyParseState();
                String name = readTypeOrIdentifier();
                ClassObj c = getClassIf(name);
                if (c != null) {
                    VarDecStatement dec = new VarDecStatement();
                    dec.type = readType(name);
                    while (true) {
                        String varName = readIdentifier();
                        Expr value = null;
                        if (readIf("=")) {
                            if (dec.type.arrayLevel > 0 && readIf("{")) {
                                value = readArrayInit(dec.type);
                            } else {
                                value = readExpr();
                            }
                        }
                        FieldObj f = new FieldObj();
                        f.isVariable = true;
                        f.type = dec.type;
                        f.name = varName;
                        localVars.put(varName, f);
                        dec.addVariable(varName, value);
                        if (readIf(";")) {
                            break;
                        }
                        read(",");
                    }
                    return dec;
                }
                current = start;
                // ExprStatement
            }
            ExprStatement stat = new ExprStatement(readExpr());
            read(";");
            return stat;
        }
    }

    private ParseState copyParseState() {
        ParseState state = new ParseState();
        state.index = current.index;
        state.line = current.line;
        state.token = current.token;
        state.type = current.type;
        return state;
    }

    private Expr readExpr() {
        Expr expr = readExpr1();
        String assign = current.token;
        if (readIf("=") || readIf("+=") || readIf("-=") || readIf("*=")
                || readIf("/=") || readIf("&=") || readIf("|=") || readIf("^=")
                || readIf("%=") || readIf("<<=") || readIf(">>=")
                || readIf(">>>=")) {
            AssignExpr assignOp = new AssignExpr();
            assignOp.left = expr;
            assignOp.op = assign;
            assignOp.right = readExpr1();
            expr = assignOp;
        }
        return expr;
    }

    private Expr readExpr1() {
        Expr expr = readExpr2();
        if (readIf("?")) {
            ConditionalExpr ce = new ConditionalExpr();
            ce.condition = expr;
            ce.ifTrue = readExpr();
            read(":");
            ce.ifFalse = readExpr();
            return ce;
        }
        return expr;
    }

    private Expr readExpr2() {
        Expr expr = readExpr2a();
        while (true) {
            String infixOp = current.token;
            if (readIf("||")) {
                OpExpr opExpr = new OpExpr(this);
                opExpr.left = expr;
                opExpr.op = infixOp;
                opExpr.right = readExpr2a();
                expr = opExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr2a() {
        Expr expr = readExpr2b();
        while (true) {
            String infixOp = current.token;
            if (readIf("&&")) {
                OpExpr opExpr = new OpExpr(this);
                opExpr.left = expr;
                opExpr.op = infixOp;
                opExpr.right = readExpr2b();
                expr = opExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr2b() {
        Expr expr = readExpr2c();
        while (true) {
            String infixOp = current.token;
            if (readIf("|")) {
                OpExpr opExpr = new OpExpr(this);
                opExpr.left = expr;
                opExpr.op = infixOp;
                opExpr.right = readExpr2c();
                expr = opExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr2c() {
        Expr expr = readExpr2d();
        while (true) {
            String infixOp = current.token;
            if (readIf("^")) {
                OpExpr opExpr = new OpExpr(this);
                opExpr.left = expr;
                opExpr.op = infixOp;
                opExpr.right = readExpr2d();
                expr = opExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr2d() {
        Expr expr = readExpr2e();
        while (true) {
            String infixOp = current.token;
            if (readIf("&")) {
                OpExpr opExpr = new OpExpr(this);
                opExpr.left = expr;
                opExpr.op = infixOp;
                opExpr.right = readExpr2e();
                expr = opExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr2e() {
        Expr expr = readExpr2f();
        while (true) {
            String infixOp = current.token;
            if (readIf("==") || readIf("!=")) {
                OpExpr opExpr = new OpExpr(this);
                opExpr.left = expr;
                opExpr.op = infixOp;
                opExpr.right = readExpr2f();
                expr = opExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr2f() {
        Expr expr = readExpr2g();
        while (true) {
            String infixOp = current.token;
            if (readIf("<") || readIf(">") || readIf("<=") || readIf(">=")) {
                OpExpr opExpr = new OpExpr(this);
                opExpr.left = expr;
                opExpr.op = infixOp;
                opExpr.right = readExpr2g();
                expr = opExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr2g() {
        Expr expr = readExpr2h();
        while (true) {
            String infixOp = current.token;
            if (readIf("<<") || readIf(">>") || readIf(">>>")) {
                OpExpr opExpr = new OpExpr(this);
                opExpr.left = expr;
                opExpr.op = infixOp;
                opExpr.right = readExpr2h();
                expr = opExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr2h() {
        Expr expr = readExpr2i();
        while (true) {
            String infixOp = current.token;
            if (readIf("+") || readIf("-")) {
                OpExpr opExpr = new OpExpr(this);
                opExpr.left = expr;
                opExpr.op = infixOp;
                opExpr.right = readExpr2i();
                expr = opExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr2i() {
        Expr expr = readExpr3();
        while (true) {
            String infixOp = current.token;
            if (readIf("*") || readIf("/") || readIf("%")) {
                OpExpr opExpr = new OpExpr(this);
                opExpr.left = expr;
                opExpr.op = infixOp;
                opExpr.right = readExpr3();
                expr = opExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr readExpr3() {
        if (readIf("(")) {
            if (isTypeOrIdentifier()) {
                ParseState start = copyParseState();
                String name = readTypeOrIdentifier();
                ClassObj c = getClassIf(name);
                if (c != null) {
                    read(")");
                    CastExpr expr = new CastExpr();
                    expr.type = new Type();
                    expr.type.classObj = c;
                    expr.expr = readExpr();
                    return expr;
                }
                current = start;
            }
            Expr expr = readExpr();
            read(")");
            return expr;
        }
        String prefix = current.token;
        if (readIf("++") || readIf("--") || readIf("!") || readIf("~")
                || readIf("+") || readIf("-")) {
            OpExpr expr = new OpExpr(this);
            expr.op = prefix;
            expr.right = readExpr3();
            return expr;
        }
        Expr expr = readExpr4();
        String suffix = current.token;
        if (readIf("++") || readIf("--")) {
            OpExpr opExpr = new OpExpr(this);
            opExpr.left = expr;
            opExpr.op = suffix;
            expr = opExpr;
        }
        return expr;
    }

    private Expr readExpr4() {
        if (readIf("false")) {
            LiteralExpr expr = new LiteralExpr(this, "boolean");
            expr.literal = "false";
            return expr;
        } else if (readIf("true")) {
            LiteralExpr expr = new LiteralExpr(this, "boolean");
            expr.literal = "true";
            return expr;
        } else if (readIf("null")) {
            LiteralExpr expr = new LiteralExpr(this, "java.lang.Object");
            expr.literal = "null";
            return expr;
        } else if (current.type == TOKEN_LITERAL_NUMBER) {
            // TODO or long, float, double
            LiteralExpr expr = new LiteralExpr(this, "int");
            expr.literal = current.token.substring(1);
            readToken();
            return expr;
        } else if (current.type == TOKEN_LITERAL_CHAR) {
            LiteralExpr expr = new LiteralExpr(this, "char");
            expr.literal = current.token + "'";
            readToken();
            return expr;
        } else if (current.type == TOKEN_LITERAL_STRING) {
            String text = current.token.substring(1);
            StringExpr expr = getStringConstant(text);
            readToken();
            return expr;
        }
        Expr expr;
        expr = readExpr5();
        while (true) {
            if (readIf(".")) {
                String n = readIdentifier();
                if (readIf("(")) {
                    CallExpr e2 = new CallExpr(this, expr, null, n);
                    if (!readIf(")")) {
                        while (true) {
                            e2.args.add(readExpr());
                            if (!readIf(",")) {
                                read(")");
                                break;
                            }
                        }
                    }
                    expr = e2;
                } else {
                    VariableExpr e2 = new VariableExpr(this);
                    e2.base = expr;
                    expr = e2;
                    e2.name = n;
                }
            } else if (readIf("[")) {
                ArrayAccessExpr arrayExpr = new ArrayAccessExpr();
                arrayExpr.base = expr;
                arrayExpr.index = readExpr();
                read("]");
                return arrayExpr;
            } else {
                break;
            }
        }
        return expr;
    }

    private StringExpr getStringConstant(String s) {
        String c = stringToStringConstantMap.get(s);
        if (c == null) {
            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < s.length() && i < 16; i++) {
                char ch = s.charAt(i);
                if (ch >= 'a' && ch <= 'z') {
                    // don't use Character.toUpperCase
                    // to avoid locale problems
                    // (the uppercase of 'i' is not always 'I')
                    buff.append((char) (ch + 'A' - 'a'));
                } else if (ch >= 'A' && ch <= 'Z') {
                    buff.append(ch);
                } else if (ch == '_' || ch == ' ') {
                    buff.append('_');
                }
            }
            c = buff.toString();
            if (c.length() == 0 || stringConstantToStringMap.containsKey(c)) {
                if (c.length() == 0) {
                    c = "X";
                }
                int i = 2;
                for (;; i++) {
                    String c2 = c + "_" + i;
                    if (!stringConstantToStringMap.containsKey(c2)) {
                        c = c2;
                        break;
                    }
                }
            }
            c = "STRING_" + c;
            stringToStringConstantMap.put(s, c);
            stringConstantToStringMap.put(c, s);
        }
        StringExpr expr = new StringExpr(this);
        expr.text = s;
        expr.constantName = c;
        return expr;
    }

    private Expr readExpr5() {
        if (readIf("new")) {
            NewExpr expr = new NewExpr();
            String typeName = readTypeOrIdentifier();
            expr.classObj = getClass(typeName);
            if (readIf("(")) {
                if (!readIf(")")) {
                    while (true) {
                        expr.args.add(readExpr());
                        if (!readIf(",")) {
                            read(")");
                            break;
                        }
                    }
                }
            } else {
                while (readIf("[")) {
                    expr.arrayInitExpr.add(readExpr());
                    read("]");
                }
            }
            return expr;
        }
        if (readIf("this")) {
            VariableExpr expr = new VariableExpr(this);
            if (thisPointer == null) {
                throw getSyntaxException("'this' used in a static context");
            }
            expr.field = thisPointer;
            return expr;
        }
        String name = readIdentifier();
        if (readIf("(")) {
            VariableExpr t;
            if (thisPointer == null) {
                // static method calling another static method
                t = null;
            } else {
                // non-static method calling a static or non-static method
                t = new VariableExpr(this);
                t.field = thisPointer;
            }
            CallExpr expr = new CallExpr(this, t, classObj.className, name);
            if (!readIf(")")) {
                while (true) {
                    expr.args.add(readExpr());
                    if (!readIf(",")) {
                        read(")");
                        break;
                    }
                }
            }
            return expr;
        }
        VariableExpr expr = new VariableExpr(this);
        FieldObj f = localVars.get(name);
        if (f == null) {
            f = method.parameters.get(name);
        }
        if (f == null) {
            f = classObj.staticFields.get(name);
        }
        if (f == null) {
            f = classObj.instanceFields.get(name);
        }
        if (f == null) {
            String imp = importMap.get(name);
            if (imp == null) {
                imp = JAVA_IMPORT_MAP.get(name);
            }
            if (imp != null) {
                name = imp;
                if (readIf(".")) {
                    String n = readIdentifier();
                    if (readIf("(")) {
                        CallExpr e2 = new CallExpr(this, null, imp, n);
                        if (!readIf(")")) {
                            while (true) {
                                e2.args.add(readExpr());
                                if (!readIf(",")) {
                                    read(")");
                                    break;
                                }
                            }
                        }
                        return e2;
                    }
                    VariableExpr e2 = new VariableExpr(this);
                    // static member variable
                    e2.name = imp + "." + n;
                    ClassObj c = classes.get(imp);
                    FieldObj sf = c.staticFields.get(n);
                    e2.field = sf;
                    return e2;
                }
                // TODO static field or method of a class
            }
        }
        expr.field = f;
        if (f != null && (!f.isVariable && !f.isStatic)) {
            VariableExpr ve = new VariableExpr(this);
            ve.field = thisPointer;
            expr.base = ve;
            if (thisPointer == null) {
                throw getSyntaxException("'this' used in a static context");
            }
        }
        expr.name = name;
        return expr;
    }

    private void read(String string) {
        if (!readIf(string)) {
            throw getSyntaxException(string + " expected, got " + current.token);
        }
    }

    private String readQualifiedIdentifier() {
        String id = readIdentifier();
        if (localVars.containsKey(id)) {
            return id;
        }
        if (classObj != null) {
            if (classObj.staticFields.containsKey(id)) {
                return id;
            }
            if (classObj.instanceFields.containsKey(id)) {
                return id;
            }
        }
        String fullName = importMap.get(id);
        if (fullName != null) {
            return fullName;
        }
        while (readIf(".")) {
            id += "." + readIdentifier();
        }
        return id;
    }

    private String readIdentifier() {
        if (current.type != TOKEN_IDENTIFIER) {
            throw getSyntaxException("identifier expected, got "
                    + current.token);
        }
        String result = current.token;
        readToken();
        return result;
    }

    private boolean readIdentifierIf(String token) {
        if (current.type == TOKEN_IDENTIFIER && token.equals(current.token)) {
            readToken();
            return true;
        }
        return false;
    }

    private boolean readIf(String token) {
        if (current.type != TOKEN_IDENTIFIER && token.equals(current.token)) {
            readToken();
            return true;
        }
        return false;
    }

    private String read() {
        String token = current.token;
        readToken();
        return token;
    }

    private RuntimeException getSyntaxException(String message) {
        return new RuntimeException(message, new ParseException(source,
                current.index));
    }

    /**
     * Replace all Unicode escapes.
     *
     * @param s the text
     * @return the cleaned text
     */
    static String replaceUnicode(String s) {
        if (s.indexOf("\\u") < 0) {
            return s;
        }
        StringBuilder buff = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            if (s.substring(i).startsWith("\\\\")) {
                buff.append("\\\\");
                i++;
            } else if (s.substring(i).startsWith("\\u")) {
                i += 2;
                while (s.charAt(i) == 'u') {
                    i++;
                }
                String c = s.substring(i, i + 4);
                buff.append((char) Integer.parseInt(c, 16));
                i += 4;
            } else {
                buff.append(s.charAt(i));
            }
        }
        return buff.toString();
    }

    /**
     * Replace all Unicode escapes and remove all remarks.
     *
     * @param s the source code
     * @return the cleaned source code
     */
    static String removeRemarks(String s) {
        char[] chars = s.toCharArray();
        for (int i = 0; i >= 0 && i < s.length(); i++) {
            if (s.charAt(i) == '\'') {
                i++;
                while (true) {
                    if (s.charAt(i) == '\\') {
                        i++;
                    } else if (s.charAt(i) == '\'') {
                        break;
                    }
                    i++;
                }
                continue;
            } else if (s.charAt(i) == '\"') {
                i++;
                while (true) {
                    if (s.charAt(i) == '\\') {
                        i++;
                    } else if (s.charAt(i) == '\"') {
                        break;
                    }
                    i++;
                }
                continue;
            }
            String sub = s.substring(i);
            if (sub.startsWith("/*") && !sub.startsWith("/* c:")) {
                int j = i;
                i = s.indexOf("*/", i + 2) + 2;
                for (; j < i; j++) {
                    if (chars[j] > ' ') {
                        chars[j] = ' ';
                    }
                }
            } else if (sub.startsWith("//") && !sub.startsWith("// c:")) {
                int j = i;
                i = s.indexOf('\n', i);
                while (j < i) {
                    chars[j++] = ' ';
                }
            }
        }
        return new String(chars) + "  ";
    }

    private void readToken() {
        int ch;
        while (true) {
            if (current.index >= source.length()) {
                current.token = null;
                return;
            }
            ch = source.charAt(current.index);
            if (ch == '\n') {
                current.line++;
            } else if (ch > ' ') {
                break;
            }
            current.index++;
        }
        int start = current.index;
        if (Character.isJavaIdentifierStart(ch)) {
            while (Character.isJavaIdentifierPart(source.charAt(current.index))) {
                current.index++;
            }
            current.token = source.substring(start, current.index);
            if (RESERVED.contains(current.token)) {
                current.type = TOKEN_RESERVED;
            } else {
                current.type = TOKEN_IDENTIFIER;
            }
            return;
        } else if (Character.isDigit(ch)
                || (ch == '.' && Character.isDigit(source
                        .charAt(current.index + 1)))) {
            String s = source.substring(current.index);
            current.token = "0" + readNumber(s);
            current.index += current.token.length() - 1;
            current.type = TOKEN_LITERAL_NUMBER;
            return;
        }
        current.index++;
        switch (ch) {
        case '\'': {
            while (true) {
                if (source.charAt(current.index) == '\\') {
                    current.index++;
                } else if (source.charAt(current.index) == '\'') {
                    break;
                }
                current.index++;
            }
            current.index++;
            current.token = source.substring(start + 1, current.index);
            current.token = "\'" + javaDecode(current.token, '\'');
            current.type = TOKEN_LITERAL_CHAR;
            return;
        }
        case '\"': {
            while (true) {
                if (source.charAt(current.index) == '\\') {
                    current.index++;
                } else if (source.charAt(current.index) == '\"') {
                    break;
                }
                current.index++;
            }
            current.index++;
            current.token = source.substring(start + 1, current.index);
            current.token = "\"" + javaDecode(current.token, '\"');
            current.type = TOKEN_LITERAL_STRING;
            return;
        }
        case '(':
        case ')':
        case '[':
        case ']':
        case '{':
        case '}':
        case ';':
        case ',':
        case '?':
        case ':':
        case '@':
            break;
        case '.':
            if (source.charAt(current.index) == '.'
                    && source.charAt(current.index + 1) == '.') {
                current.index += 2;
            }
            break;
        case '+':
            if (source.charAt(current.index) == '='
                    || source.charAt(current.index) == '+') {
                current.index++;
            }
            break;
        case '-':
            if (source.charAt(current.index) == '='
                    || source.charAt(current.index) == '-') {
                current.index++;
            }
            break;
        case '>':
            if (source.charAt(current.index) == '>') {
                current.index++;
                if (source.charAt(current.index) == '>') {
                    current.index++;
                }
            }
            if (source.charAt(current.index) == '=') {
                current.index++;
            }
            break;
        case '<':
            if (source.charAt(current.index) == '<') {
                current.index++;
            }
            if (source.charAt(current.index) == '=') {
                current.index++;
            }
            break;
        case '/':
            if (source.charAt(current.index) == '*'
                    || source.charAt(current.index) == '/'
                    || source.charAt(current.index) == '=') {
                current.index++;
            }
            break;
        case '*':
        case '~':
        case '!':
        case '=':
        case '%':
        case '^':
            if (source.charAt(current.index) == '=') {
                current.index++;
            }
            break;
        case '&':
            if (source.charAt(current.index) == '&') {
                current.index++;
            } else if (source.charAt(current.index) == '=') {
                current.index++;
            }
            break;
        case '|':
            if (source.charAt(current.index) == '|') {
                current.index++;
            } else if (source.charAt(current.index) == '=') {
                current.index++;
            }
            break;
        }
        current.type = TOKEN_OTHER;
        current.token = source.substring(start, current.index);
    }

    /**
     * Parse a number literal and returns it.
     *
     * @param s the source code
     * @return the number
     */
    static String readNumber(String s) {
        int i = 0;
        if (s.startsWith("0x") || s.startsWith("0X")) {
            i = 2;
            while (true) {
                char ch = s.charAt(i);
                if ((ch < '0' || ch > '9') && (ch < 'a' || ch > 'f')
                        && (ch < 'A' || ch > 'F')) {
                    break;
                }
                i++;
            }
            if (s.charAt(i) == 'l' || s.charAt(i) == 'L') {
                i++;
            }
        } else {
            while (true) {
                char ch = s.charAt(i);
                if ((ch < '0' || ch > '9') && ch != '.') {
                    break;
                }
                i++;
            }
            if (s.charAt(i) == 'e' || s.charAt(i) == 'E') {
                i++;
                if (s.charAt(i) == '-' || s.charAt(i) == '+') {
                    i++;
                }
                while (Character.isDigit(s.charAt(i))) {
                    i++;
                }
            }
            if (s.charAt(i) == 'f' || s.charAt(i) == 'F' || s.charAt(i) == 'd'
                    || s.charAt(i) == 'D' || s.charAt(i) == 'L'
                    || s.charAt(i) == 'l') {
                i++;
            }
        }
        return s.substring(0, i);
    }

    private static RuntimeException getFormatException(String s, int i) {
        return new RuntimeException(new ParseException(s, i));
    }

    private static String javaDecode(String s, char end) {
        StringBuilder buff = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == end) {
                break;
            } else if (c == '\\') {
                if (i >= s.length()) {
                    throw getFormatException(s, s.length() - 1);
                }
                c = s.charAt(++i);
                switch (c) {
                case 't':
                    buff.append('\t');
                    break;
                case 'r':
                    buff.append('\r');
                    break;
                case 'n':
                    buff.append('\n');
                    break;
                case 'b':
                    buff.append('\b');
                    break;
                case 'f':
                    buff.append('\f');
                    break;
                case '"':
                    buff.append('"');
                    break;
                case '\'':
                    buff.append('\'');
                    break;
                case '\\':
                    buff.append('\\');
                    break;
                case 'u': {
                    try {
                        c = (char) (Integer.parseInt(s.substring(i + 1, i + 5),
                                16));
                    } catch (NumberFormatException e) {
                        throw getFormatException(s, i);
                    }
                    i += 4;
                    buff.append(c);
                    break;
                }
                default:
                    if (c >= '0' && c <= '9') {
                        try {
                            c = (char) (Integer.parseInt(s.substring(i, i + 3),
                                    8));
                        } catch (NumberFormatException e) {
                            throw getFormatException(s, i);
                        }
                        i += 2;
                        buff.append(c);
                    } else {
                        throw getFormatException(s, i);
                    }
                }
            } else {
                buff.append(c);
            }
        }
        return buff.toString();
    }

    /**
     * Write the C++ header.
     *
     * @param out the output writer
     */
    void writeHeader(PrintWriter out) {
        for (Statement s : nativeHeaders) {
            out.println(s.asString());
        }
        if (JavaParser.REF_COUNT_STATIC) {
            out.println("#define STRING(s) STRING_REF(s)");
        } else {
            out.println("#define STRING(s) STRING_PTR(s)");
        }
        out.println();
        for (ClassObj c : classes.values()) {
            out.println("class " + toC(c.className) + ";");
        }
        for (ClassObj c : classes.values()) {
            for (FieldObj f : c.staticFields.values()) {
                StringBuilder buff = new StringBuilder();
                buff.append("extern ");
                if (f.isFinal) {
                    buff.append("const ");
                }
                buff.append(f.type.asString());
                buff.append(" ").append(toC(c.className + "." + f.name));
                buff.append(";");
                out.println(buff.toString());
            }
            for (ArrayList<MethodObj> list : c.methods.values()) {
                for (MethodObj m : list) {
                    if (m.isIgnore) {
                        continue;
                    }
                    if (m.isStatic) {
                        out.print(m.returnType.asString());
                        out.print(" " + toC(c.className + "_" + m.name) + "(");
                        int i = 0;
                        for (FieldObj p : m.parameters.values()) {
                            if (i > 0) {
                                out.print(", ");
                            }
                            out.print(p.type.asString() + " " + p.name);
                            i++;
                        }
                        out.println(");");
                    }
                }
            }
            out.print("class " + toC(c.className) + " : public ");
            if (c.superClassName == null) {
                if (c.className.equals("java.lang.Object")) {
                    out.print("RefBase");
                } else {
                    out.print("java_lang_Object");
                }
            } else {
                out.print(toC(c.superClassName));
            }
            out.println(" {");
            out.println("public:");
            for (FieldObj f : c.instanceFields.values()) {
                out.print("    ");
                out.print(f.type.asString() + " " + f.name);
                out.println(";");
            }
            out.println("public:");
            for (ArrayList<MethodObj> list : c.methods.values()) {
                for (MethodObj m : list) {
                    if (m.isIgnore) {
                        continue;
                    }
                    if (m.isStatic) {
                        continue;
                    }
                    if (m.isConstructor) {
                        out.print("    " + toC(c.className) + "(");
                    } else {
                        out.print("    " + m.returnType.asString() + " "
                                + m.name + "(");
                    }
                    int i = 0;
                    for (FieldObj p : m.parameters.values()) {
                        if (i > 0) {
                            out.print(", ");
                        }
                        out.print(p.type.asString());
                        out.print(" " + p.name);
                        i++;
                    }
                    out.println(");");
                }
            }
            out.println("};");
        }
        ArrayList<String> constantNames = new ArrayList<>(stringConstantToStringMap.keySet());
        Collections.sort(constantNames);
        for (String c : constantNames) {
            String s = stringConstantToStringMap.get(c);
            if (JavaParser.REF_COUNT_STATIC) {
                out.println("ptr<java_lang_String> " + c + " = STRING(L\"" + s
                        + "\");");
            } else {
                out.println("java_lang_String* " + c + " = STRING(L\"" + s
                        + "\");");
            }
        }
    }

    /**
     * Write the C++ source code.
     *
     * @param out the output writer
     */
    void writeSource(PrintWriter out) {
        for (ClassObj c : classes.values()) {
            out.println("/* " + c.className + " */");
            for (Statement s : c.nativeCode) {
                out.println(s.asString());
            }
            for (FieldObj f : c.staticFields.values()) {
                StringBuilder buff = new StringBuilder();
                if (f.isFinal) {
                    buff.append("const ");
                }
                buff.append(f.type.asString());
                buff.append(" ").append(toC(c.className + "." + f.name));
                if (f.value != null) {
                    buff.append(" = ").append(f.value.asString());
                }
                buff.append(";");
                out.println(buff.toString());
            }
            for (ArrayList<MethodObj> list : c.methods.values()) {
                for (MethodObj m : list) {
                    if (m.isIgnore) {
                        continue;
                    }
                    if (m.isStatic) {
                        out.print(m.returnType.asString() + " "
                                + toC(c.className + "_" + m.name) + "(");
                    } else if (m.isConstructor) {
                        out.print(toC(c.className) + "::" + toC(c.className)
                                + "(");
                    } else {
                        out.print(m.returnType.asString() + " "
                                + toC(c.className) + "::" + m.name + "(");
                    }
                    int i = 0;
                    for (FieldObj p : m.parameters.values()) {
                        if (i > 0) {
                            out.print(", ");
                        }
                        out.print(p.type.asString() + " " + p.name);
                        i++;
                    }
                    out.println(") {");
                    if (m.isConstructor) {
                        for (FieldObj f : c.instanceFields.values()) {
                            out.print("    ");
                            out.print("this->" + f.name);
                            out.print(" = " + f.value.asString());
                            out.println(";");
                        }
                    }
                    if (m.block != null) {
                        m.block.setMethod(m);
                        out.print(m.block.asString());
                    }
                    out.println("}");
                    out.println();
                }
            }
        }
    }

    private static String indent(String s, int spaces) {
        StringBuilder buff = new StringBuilder(s.length() + spaces);
        for (int i = 0; i < s.length();) {
            for (int j = 0; j < spaces; j++) {
                buff.append(' ');
            }
            int n = s.indexOf('\n', i);
            n = n < 0 ? s.length() : n + 1;
            buff.append(s.substring(i, n));
            i = n;
        }
        if (!s.endsWith("\n")) {
            buff.append('\n');
        }
        return buff.toString();
    }

    /**
     * Move the source code 4 levels to the right.
     *
     * @param o the source code
     * @return the indented code
     */
    static String indent(String o) {
        return indent(o, 4);
    }

    /**
     * Get the C++ representation of this identifier.
     *
     * @param identifier the identifier
     * @return the C representation
     */
    static String toC(String identifier) {
        return identifier.replace('.', '_');
    }

    ClassObj getClassObj() {
        return classObj;
    }

    /**
     * Get the class of the given name.
     *
     * @param className the name
     * @return the class
     */
    ClassObj getClassObj(String className) {
        ClassObj c = BUILT_IN_CLASSES.get(className);
        if (c == null) {
            c = classes.get(className);
        }
        return c;
    }

}

/**
 * The parse state.
 */
class ParseState {

    /**
     * The parse index.
     */
    int index;

    /**
     * The token type
     */
    int type;

    /**
     * The token text.
     */
    String token;

    /**
     * The line number.
     */
    int line;
}