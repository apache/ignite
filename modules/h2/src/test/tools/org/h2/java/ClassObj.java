/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;

import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * A class or interface.
 */
public class ClassObj {

    /**
     * The super class (null for java.lang.Object or primitive types).
     */
    String superClassName;

    /**
     * The list of interfaces that this class implements.
     */
    ArrayList<String> interfaceNames = new ArrayList<>();


    /**
     * The fully qualified class name.
     */
    String className;

    /**
     * Whether this is an interface.
     */
    boolean isInterface;

    /**
     * Whether this class is public.
     */
    boolean isPublic;

    /**
     * Whether this is a primitive class (int, char,...)
     */
    boolean isPrimitive;

    /**
     * The primitive type (higher types are more complex)
     */
    int primitiveType;

    /**
     * The imported classes.
     */
    ArrayList<ClassObj> imports = new ArrayList<>();

    /**
     * The per-instance fields.
     */
    LinkedHashMap<String, FieldObj> instanceFields =
            new LinkedHashMap<>();

    /**
     * The static fields of this class.
     */
    LinkedHashMap<String, FieldObj> staticFields =
            new LinkedHashMap<>();

    /**
     * The methods.
     */
    LinkedHashMap<String, ArrayList<MethodObj>> methods =
            new LinkedHashMap<>();

    /**
     * The list of native statements.
     */
    ArrayList<Statement> nativeCode = new ArrayList<>();

    /**
     * The class number.
     */
    int id;

    /**
     * Get the base type of this class.
     */
    Type baseType;

    ClassObj() {
        baseType = new Type();
        baseType.classObj = this;
    }

    /**
     * Add a method.
     *
     * @param method the method
     */
    void addMethod(MethodObj method) {
        ArrayList<MethodObj> list = methods.get(method.name);
        if (list == null) {
            list = new ArrayList<>();
            methods.put(method.name, list);
        } else {
            // for overloaded methods
            // method.name = method.name + "_" + (list.size() + 1);
        }
        list.add(method);
    }

    /**
     * Add an instance field.
     *
     * @param field the field
     */
    void addInstanceField(FieldObj field) {
        instanceFields.put(field.name, field);
    }

    /**
     * Add a static field.
     *
     * @param field the field
     */
    void addStaticField(FieldObj field) {
        staticFields.put(field.name, field);
    }

    @Override
    public String toString() {
        if (isPrimitive) {
            return "j" + className;
        }
        return JavaParser.toC(className);
    }

    /**
     * Get the method.
     *
     * @param find the method name in the source code
     * @param args the parameters
     * @return the method
     */
    MethodObj getMethod(String find, ArrayList<Expr> args) {
        ArrayList<MethodObj> list = methods.get(find);
        if (list == null) {
            throw new RuntimeException("Method not found: " + className + " " + find);
        }
        if (list.size() == 1) {
            return list.get(0);
        }
        for (MethodObj m : list) {
            if (!m.isVarArgs && m.parameters.size() != args.size()) {
                continue;
            }
            boolean match = true;
            int i = 0;
            for (FieldObj f : m.parameters.values()) {
                Expr a = args.get(i++);
                Type t = a.getType();
                if (!t.equals(f.type)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return m;
            }
        }
        throw new RuntimeException("Method not found: " + className);
    }

    /**
     * Get the field with the given name.
     *
     * @param name the field name
     * @return the field
     */
    FieldObj getField(String name) {
        return instanceFields.get(name);
    }

    @Override
    public int hashCode() {
        return className.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ClassObj) {
            ClassObj c = (ClassObj) other;
            return c.className.equals(className);
        }
        return false;
    }

}

/**
 * A method.
 */
class MethodObj {

    /**
     * Whether the last parameter is a var args parameter.
     */
    boolean isVarArgs;

    /**
     * Whether this method is static.
     */
    boolean isStatic;

    /**
     * Whether this method is private.
     */
    boolean isPrivate;

    /**
     * Whether this method is overridden.
     */
    boolean isVirtual;

    /**
     * Whether this method is to be ignored (using the Ignore annotation).
     */
    boolean isIgnore;

    /**
     * The name.
     */
    String name;

    /**
     * The statement block (if any).
     */
    Statement block;

    /**
     * The return type.
     */
    Type returnType;

    /**
     * The parameter list.
     */
    LinkedHashMap<String, FieldObj> parameters =
            new LinkedHashMap<>();

    /**
     * Whether this method is final.
     */
    boolean isFinal;

    /**
     * Whether this method is public.
     */
    boolean isPublic;

    /**
     * Whether this method is native.
     */
    boolean isNative;

    /**
     * Whether this is a constructor.
     */
    boolean isConstructor;

    @Override
    public String toString() {
        return name;
    }

}

/**
 * A field.
 */
class FieldObj {

    /**
     * The type.
     */
    Type type;

    /**
     * Whether this is a variable or parameter.
     */
    boolean isVariable;

    /**
     * Whether this is a local field (not separately garbage collected).
     */
    boolean isLocalField;

    /**
     * The field name.
     */
    String name;

    /**
     * Whether this field is static.
     */
    boolean isStatic;

    /**
     * Whether this field is final.
     */
    boolean isFinal;

    /**
     * Whether this field is private.
     */
    boolean isPrivate;

    /**
     * Whether this field is public.
     */
    boolean isPublic;

    /**
     * Whether this method is to be ignored (using the Ignore annotation).
     */
    boolean isIgnore;

    /**
     * The initial value expression (may be null).
     */
    Expr value;

    /**
     * The class where this field is declared.
     */
    ClassObj declaredClass;

    @Override
    public String toString() {
        return name;
    }

}

/**
 * A type.
 */
class Type {

    /**
     * The class.
     */
    ClassObj classObj;

    /**
     * The array nesting level. 0 if not an array.
     */
    int arrayLevel;

    /**
     * Whether this is a var args parameter.
     */
    boolean isVarArgs;

    /**
     * Use ref-counting.
     */
    boolean refCount = JavaParser.REF_COUNT;

    /**
     * Whether this is a array or an non-primitive type.
     *
     * @return true if yes
     */
    public boolean isObject() {
        return arrayLevel > 0 || !classObj.isPrimitive;
    }

    @Override
    public String toString() {
        return asString();
    }

    /**
     * Get the C++ code.
     *
     * @return the C++ code
     */
    public String asString() {
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < arrayLevel; i++) {
            if (refCount) {
                buff.append("ptr< ");
            }
            buff.append("array< ");
        }
        if (refCount) {
            if (!classObj.isPrimitive) {
                buff.append("ptr< ");
            }
        }
        buff.append(classObj.toString());
        if (refCount) {
            if (!classObj.isPrimitive) {
                buff.append(" >");
            }
        }
        for (int i = 0; i < arrayLevel; i++) {
            if (refCount) {
                buff.append(" >");
            } else {
                if (!classObj.isPrimitive) {
                    buff.append("*");
                }
            }
            buff.append(" >");
        }
        if (!refCount) {
            if (isObject()) {
                buff.append("*");
            }
        }
        return buff.toString();
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Type) {
            Type t = (Type) other;
            return t.classObj.equals(classObj) && t.arrayLevel == arrayLevel
                    && t.isVarArgs == isVarArgs;
        }
        return false;
    }

    /**
     * Get the default value, for primitive types (0 usually).
     *
     * @param context the context
     * @return the expression
     */
    public Expr getDefaultValue(JavaParser context) {
        if (classObj.isPrimitive) {
            LiteralExpr literal = new LiteralExpr(context, classObj.className);
            literal.literal = "0";
            CastExpr cast = new CastExpr();
            cast.type = this;
            cast.expr = literal;
            cast.type = this;
            return cast;
        }
        LiteralExpr literal = new LiteralExpr(context, classObj.className);
        literal.literal = "null";
        return literal;
    }

}

