/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * An expression.
 */
public interface Expr {

    /**
     * Get the C++ code.
     *
     * @return the C++ code
     */
    String asString();

    Type getType();
    void setType(Type type);

}

/**
 * The base expression class.
 */
abstract class ExprBase implements Expr {
    @Override
    public final String toString() {
        return "_" + asString() + "_";
    }
}

/**
 * A method call.
 */
class CallExpr extends ExprBase {

    /**
     * The parameters.
     */
    final ArrayList<Expr> args = new ArrayList<>();

    private final JavaParser context;
    private final String className;
    private final String name;
    private Expr expr;
    private ClassObj classObj;
    private MethodObj method;
    private Type type;

    CallExpr(JavaParser context, Expr expr, String className, String name) {
        this.context = context;
        this.expr = expr;
        this.className = className;
        this.name = name;
    }

    private void initMethod() {
        if (method != null) {
            return;
        }
        if (className != null) {
            classObj = context.getClassObj(className);
        } else {
            classObj = expr.getType().classObj;
        }
        method = classObj.getMethod(name, args);
        if (method.isStatic) {
            expr = null;
        }
    }

    @Override
    public String asString() {
        StringBuilder buff = new StringBuilder();
        initMethod();
        if (method.isIgnore) {
            if (args.size() == 0) {
                // ignore
            } else if (args.size() == 1) {
                buff.append(args.get(0));
            } else {
                throw new IllegalArgumentException(
                        "Cannot ignore method with multiple arguments: " + method);
            }
        } else {
            if (expr == null) {
                // static method
                buff.append(JavaParser.toC(classObj.toString() + "." + method.name));
            } else {
                buff.append(expr.asString()).append("->");
                buff.append(method.name);
            }
            buff.append("(");
            int i = 0;
            Iterator<FieldObj> paramIt = method.parameters.values().iterator();
            for (Expr a : args) {
                if (i > 0) {
                    buff.append(", ");
                }
                FieldObj f = paramIt.next();
                i++;
                a.setType(f.type);
                buff.append(a.asString());
            }
            buff.append(")");
        }
        return buff.toString();
    }

    @Override
    public Type getType() {
        initMethod();
        return method.returnType;
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

}

/**
 * A assignment expression.
 */
class AssignExpr extends ExprBase {

    /**
     * The target variable or field.
     */
    Expr left;

    /**
     * The operation (=, +=,...).
     */
    String op;

    /**
     * The expression.
     */
    Expr right;

    /**
     * The type.
     */
    Type type;

    @Override
    public String asString() {
        right.setType(left.getType());
        return left.asString() + " " + op + " " + right.asString();
    }

    @Override
    public Type getType() {
        return left.getType();
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

}

/**
 * A conditional expression.
 */
class ConditionalExpr extends ExprBase {

    /**
     * The condition.
     */
    Expr condition;

    /**
     * The 'true' expression.
     */
    Expr ifTrue;

    /**
     * The 'false' expression.
     */
    Expr ifFalse;

    @Override
    public String asString() {
        return condition.asString() + " ? " + ifTrue.asString() + " : "
                + ifFalse.asString();
    }

    @Override
    public Type getType() {
        return ifTrue.getType();
    }

    @Override
    public void setType(Type type) {
        ifTrue.setType(type);
        ifFalse.setType(type);
    }

}

/**
 * A literal.
 */
class LiteralExpr extends ExprBase {

    /**
     * The literal expression.
     */
    String literal;

    private final JavaParser context;
    private final String className;
    private Type type;

    public LiteralExpr(JavaParser context, String className) {
        this.context = context;
        this.className = className;
    }

    @Override
    public String asString() {
        if ("null".equals(literal)) {
            Type t = getType();
            if (t.isObject()) {
                return "(" + getType().asString() + ") 0";
            }
            return t.asString() + "()";
        }
        return literal;
    }

    @Override
    public Type getType() {
        if (type == null) {
            type = new Type();
            type.classObj = context.getClassObj(className);
        }
        return type;
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

}

/**
 * An operation.
 */
class OpExpr extends ExprBase {

    /**
     * The left hand side.
     */
    Expr left;

    /**
     * The operation.
     */
    String op;

    /**
     * The right hand side.
     */
    Expr right;

    private final JavaParser context;
    private Type type;

    OpExpr(JavaParser context) {
        this.context = context;
    }

    @Override
    public String asString() {
        if (left == null) {
            return op + right.asString();
        } else if (right == null) {
            return left.asString() + op;
        }
        if (op.equals(">>>")) {
            // ujint / ujlong
            return "(((u" + left.getType() + ") " + left + ") >> " + right + ")";
        } else if (op.equals("+")) {
            if (left.getType().isObject() || right.getType().isObject()) {
                // TODO convert primitive to to String, call toString
                StringBuilder buff = new StringBuilder();
                if (type.refCount) {
                    buff.append("ptr<java_lang_StringBuilder>(new java_lang_StringBuilder(");
                } else {
                    buff.append("(new java_lang_StringBuilder(");
                }
                buff.append(convertToString(left));
                buff.append("))->append(");
                buff.append(convertToString(right));
                buff.append(")->toString()");
                return buff.toString();
            }
        }
        return "(" + left.asString() + " " + op + " " + right.asString() + ")";
    }

    private String convertToString(Expr e) {
        Type t = e.getType();
        if (t.arrayLevel > 0) {
            return e.toString() + "->toString()";
        }
        if (t.classObj.isPrimitive) {
            ClassObj wrapper = context.getWrapper(t.classObj);
            return JavaParser.toC(wrapper + ".toString") + "(" + e.asString() + ")";
        } else if (e.getType().asString().equals("java_lang_String*")) {
            return e.asString();
        }
        return e.asString() + "->toString()";
    }

    private static boolean isComparison(String op) {
        return op.equals("==") || op.equals(">") || op.equals("<") ||
                op.equals(">=") || op.equals("<=") || op.equals("!=");
    }

    @Override
    public Type getType() {
        if (left == null) {
            return right.getType();
        }
        if (right == null) {
            return left.getType();
        }
        if (isComparison(op)) {
            Type t = new Type();
            t.classObj = JavaParser.getBuiltInClass("boolean");
            return t;
        }
        if (op.equals("+")) {
            if (left.getType().isObject() || right.getType().isObject()) {
                Type t = new Type();
                t.classObj = context.getClassObj("java.lang.String");
                return t;
            }
        }
        Type lt = left.getType();
        Type rt = right.getType();
        if (lt.classObj.primitiveType < rt.classObj.primitiveType) {
            return rt;
        }
        return lt;
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

}

/**
 * A "new" expression.
 */
class NewExpr extends ExprBase {

    /**
     * The class.
     */
    ClassObj classObj;

    /**
     * The constructor parameters (for objects).
     */
    final ArrayList<Expr> args = new ArrayList<>();

    /**
     * The array bounds (for arrays).
     */
    final ArrayList<Expr> arrayInitExpr = new ArrayList<>();

    /**
     * The type.
     */
    Type type;

    @Override
    public String asString() {
        boolean refCount = type.refCount;
        StringBuilder buff = new StringBuilder();
        if (arrayInitExpr.size() > 0) {
            if (refCount) {
                if (classObj.isPrimitive) {
                    buff.append("ptr< array< " + classObj + " > >");
                } else {
                    buff.append("ptr< array< ptr< " + classObj + " > > >");
                }
            }
            if (classObj.isPrimitive) {
                buff.append("(new array< " + classObj + " >(1 ");
            } else {
                if (refCount) {
                    buff.append("(new array< ptr< " + classObj + " > >(1 ");
                } else {
                    buff.append("(new array< " + classObj + "* >(1 ");
                }
            }
            for (Expr e : arrayInitExpr) {
                buff.append("* ").append(e.asString());
            }
            buff.append("))");
        } else {
            if (refCount) {
                buff.append("ptr< " + classObj + " >");
            }
            buff.append("(new " + classObj);
            buff.append("(");
            int i = 0;
            for (Expr a : args) {
                if (i++ > 0) {
                    buff.append(", ");
                }
                buff.append(a.asString());
            }
            buff.append("))");
        }
        return buff.toString();
    }

    @Override
    public Type getType() {
        Type t = new Type();
        t.classObj = classObj;
        t.arrayLevel = arrayInitExpr.size();
        return t;
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

}

/**
 * A String literal.
 */
class StringExpr extends ExprBase {

    /**
     * The constant name.
     */
    String constantName;

    /**
     * The literal.
     */
    String text;

    private final JavaParser context;
    private Type type;

    StringExpr(JavaParser context) {
        this.context = context;
    }

    @Override
    public String asString() {
        return constantName;
    }

    @Override
    public Type getType() {
        if (type == null) {
            type = new Type();
            type.classObj = context.getClassObj("java.lang.String");
        }
        return type;
    }

    /**
     * Encode the String to Java syntax.
     *
     * @param s the string
     * @return the encoded string
     */
    static String javaEncode(String s) {
        StringBuilder buff = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
            case '\t':
                // HT horizontal tab
                buff.append("\\t");
                break;
            case '\n':
                // LF linefeed
                buff.append("\\n");
                break;
            case '\f':
                // FF form feed
                buff.append("\\f");
                break;
            case '\r':
                // CR carriage return
                buff.append("\\r");
                break;
            case '"':
                // double quote
                buff.append("\\\"");
                break;
            case '\\':
                // backslash
                buff.append("\\\\");
                break;
            default:
                int ch = c & 0xffff;
                if (ch >= ' ' && (ch < 0x80)) {
                    buff.append(c);
                // not supported in properties files
                // } else if(ch < 0xff) {
                // buff.append("\\");
                // // make sure it's three characters (0x200 is octal 1000)
                // buff.append(Integer.toOctalString(0x200 | ch).substring(1));
                } else {
                    buff.append("\\u");
                    // make sure it's four characters
                    buff.append(Integer.toHexString(0x10000 | ch).substring(1));
                }
            }
        }
        return buff.toString();
    }

    @Override
    public void setType(Type type) {
        // ignore
    }

}

/**
 * A variable.
 */
class VariableExpr extends ExprBase {

    /**
     * The variable name.
     */
    String name;

    /**
     * The base expression (the first element in a.b variables).
     */
    Expr base;

    /**
     * The field.
     */
    FieldObj field;

    private Type type;
    private final JavaParser context;

    VariableExpr(JavaParser context) {
        this.context = context;
    }

    @Override
    public String asString() {
        init();
        StringBuilder buff = new StringBuilder();
        if (base != null) {
            buff.append(base.asString()).append("->");
        }
        if (field != null) {
            if (field.isStatic) {
                buff.append(JavaParser.toC(field.declaredClass + "." + field.name));
            } else if (field.name != null) {
                buff.append(field.name);
            } else if ("length".equals(name) && base.getType().arrayLevel > 0) {
                buff.append("length()");
            }
        } else {
            buff.append(JavaParser.toC(name));
        }
        return buff.toString();
    }

    private void init() {
        if (field == null) {
            Type t = base.getType();
            if (t.arrayLevel > 0) {
                if ("length".equals(name)) {
                    field = new FieldObj();
                    field.type = context.getClassObj("int").baseType;
                } else {
                    throw new IllegalArgumentException("Unknown array method: " + name);
                }
            } else {
                field = t.classObj.getField(name);
            }
        }
    }

    @Override
    public Type getType() {
        init();
        return field.type;
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

}

/**
 * An array initializer expression.
 */
class ArrayInitExpr extends ExprBase {

    /**
     * The expression list.
     */
    final ArrayList<Expr> list = new ArrayList<>();

    /**
     * The type.
     */
    Type type;

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public String asString() {
        StringBuilder buff = new StringBuilder("{ ");
        int i = 0;
        for (Expr e : list) {
            if (i++ > 0) {
                buff.append(", ");
            }
            buff.append(e.toString());
        }
        buff.append(" }");
        return buff.toString();
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

}

/**
 * A type cast expression.
 */
class CastExpr extends ExprBase {

    /**
     * The expression.
     */
    Expr expr;

    /**
     * The cast type.
     */
    Type type;

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public String asString() {
        return "(" + type.asString() + ") " + expr.asString();
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

}

/**
 * An array access expression (get or set).
 */
class ArrayAccessExpr extends ExprBase {

    /**
     * The base expression.
     */
    Expr base;

    /**
     * The index.
     */
    Expr index;

    /**
     * The type.
     */
    Type type;

    @Override
    public Type getType() {
        Type t = new Type();
        t.classObj = base.getType().classObj;
        t.arrayLevel = base.getType().arrayLevel - 1;
        return t;
    }

    @Override
    public String asString() {
        return base.asString() + "->at(" + index.asString() + ")";
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

}
