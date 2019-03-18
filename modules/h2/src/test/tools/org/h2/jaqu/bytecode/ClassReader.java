/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.bytecode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import org.h2.jaqu.Token;

/**
 * This class converts a method to a SQL Token by interpreting
 * (decompiling) the bytecode of the class.
 */
public class ClassReader {

    private static final boolean DEBUG = false;

    private byte[] data;
    private int pos;
    private Constant[] constantPool;
    private int startByteCode;
    private String methodName;

    private String convertMethodName;
    private Token result;
    private Stack<Token> stack = new Stack<>();
    private ArrayList<Token> variables = new ArrayList<>();
    private boolean endOfMethod;
    private boolean condition;
    private int nextPc;
    private Map<String, Object> fieldMap = new HashMap<>();

    private static void debug(String s) {
        if (DEBUG) {
            System.out.println(s);
        }
    }

    public Token decompile(Object instance, Map<String, Object> fields,
            String method) {
        this.fieldMap = fields;
        this.convertMethodName = method;
        Class<?> clazz = instance.getClass();
        String className = clazz.getName();
        debug("class name " + className);
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        try {
            InputStream in = clazz.getClassLoader()
                    .getResource(className.replace('.', '/') + ".class")
                    .openStream();
            while (true) {
                int x = in.read();
                if (x < 0) {
                    break;
                }
                buff.write(x);
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not read class bytecode", e);
        }
        data = buff.toByteArray();
        int header = readInt();
        debug("header: " + Integer.toHexString(header));
        int minorVersion = readShort();
        int majorVersion = readShort();
        debug("version: " + majorVersion + "." + minorVersion);
        int constantPoolCount = readShort();
        constantPool = new Constant[constantPoolCount];
        for (int i = 1; i < constantPoolCount; i++) {
            int type = readByte();
            switch (type) {
            case 1:
                constantPool[i] = ConstantString.get(readString());
                break;
            case 3: {
                int x = readInt();
                constantPool[i] = ConstantNumber.get(x);
                break;
            }
            case 4: {
                int x = readInt();
                constantPool[i] = ConstantNumber.get(
                        "" + Float.intBitsToFloat(x), x, Constant.Type.FLOAT);
                break;
            }
            case 5: {
                long x = readLong();
                constantPool[i] = ConstantNumber.get(x);
                i++;
                break;
            }
            case 6: {
                long x = readLong();
                constantPool[i] = ConstantNumber.get(
                        "" + Double.longBitsToDouble(x), x,
                        Constant.Type.DOUBLE);
                i++;
                break;
            }
            case 7: {
                int x = readShort();
                constantPool[i] = ConstantNumber.get(null, x,
                        ConstantNumber.Type.CLASS_REF);
                break;
            }
            case 8: {
                int x = readShort();
                constantPool[i] = ConstantNumber.get(null, x,
                        ConstantNumber.Type.STRING_REF);
                break;
            }
            case 9: {
                int x = readInt();
                constantPool[i] = ConstantNumber.get(null, x,
                        ConstantNumber.Type.FIELD_REF);
                break;
            }
            case 10: {
                int x = readInt();
                constantPool[i] = ConstantNumber.get(null, x,
                        ConstantNumber.Type.METHOD_REF);
                break;
            }
            case 11: {
                int x = readInt();
                constantPool[i] = ConstantNumber.get(null, x,
                        ConstantNumber.Type.INTERFACE_METHOD_REF);
                break;
            }
            case 12: {
                int x = readInt();
                constantPool[i] = ConstantNumber.get(null, x,
                        ConstantNumber.Type.NAME_AND_TYPE);
                break;
            }
            default:
                throw new RuntimeException("Unsupported constant pool tag: " + type);
            }
        }
        int accessFlags = readShort();
        debug("access flags: " + accessFlags);
        int classRef = readShort();
        debug("class: " + constantPool[constantPool[classRef].intValue()]);
        int superClassRef = readShort();
        debug(" extends " + constantPool[constantPool[superClassRef].intValue()]);
        int interfaceCount = readShort();
        for (int i = 0; i < interfaceCount; i++) {
            int interfaceRef = readShort();
            debug(" implements " + constantPool[constantPool[interfaceRef].intValue()]);
        }
        int fieldCount = readShort();
        for (int i = 0; i < fieldCount; i++) {
            readField();
        }
        int methodCount = readShort();
        for (int i = 0; i < methodCount; i++) {
            readMethod();
        }
        readAttributes();
        return result;
    }

    private void readField() {
        int accessFlags = readShort();
        int nameIndex = readShort();
        int descIndex = readShort();
        debug("    " + constantPool[descIndex] + " " + constantPool[nameIndex]
                + " " + accessFlags);
        readAttributes();
    }

    private void readMethod() {
        int accessFlags = readShort();
        int nameIndex = readShort();
        int descIndex = readShort();
        String desc = constantPool[descIndex].toString();
        methodName = constantPool[nameIndex].toString();
        debug("    " + desc + " " + methodName + " " + accessFlags);
        readAttributes();
    }

    private void readAttributes() {
        int attributeCount = readShort();
        for (int i = 0; i < attributeCount; i++) {
            int attributeNameIndex = readShort();
            String attributeName = constantPool[attributeNameIndex].toString();
            debug("        attribute " + attributeName);
            int attributeLength = readInt();
            int end = pos + attributeLength;
            if ("Code".equals(attributeName)) {
                readCode();
            }
            pos = end;
        }
    }

    void decompile() {
        int maxStack = readShort();
        int maxLocals = readShort();
        debug("stack: " + maxStack + " locals: " + maxLocals);
        int codeLength = readInt();
        startByteCode = pos;
        int end = pos + codeLength;
        while (pos < end) {
            readByteCode();
        }
        debug("");
        pos = startByteCode + codeLength;
        int exceptionTableLength = readShort();
        pos += 2 * exceptionTableLength;
        readAttributes();
    }

    private void readCode() {
        variables.clear();
        stack.clear();
        int maxStack = readShort();
        int maxLocals = readShort();
        debug("stack: " + maxStack + " locals: " + maxLocals);
        int codeLength = readInt();
        startByteCode = pos;
        if (methodName.startsWith(convertMethodName)) {
            result = getResult();
        }
        pos = startByteCode + codeLength;
        int exceptionTableLength = readShort();
        pos += 2 * exceptionTableLength;
        readAttributes();
    }

    @SuppressWarnings("unlikely-arg-type")
    private Token getResult() {
        while (true) {
            readByteCode();
            if (endOfMethod) {
                return stack.pop();
            }
            if (condition) {
                Token c = stack.pop();
                Stack<Token> currentStack = new Stack<>();
                currentStack.addAll(stack);
                ArrayList<Token> currentVariables = new ArrayList<>(variables);
                int branch = nextPc;
                Token a = getResult();
                stack = currentStack;
                variables = currentVariables;
                pos = branch + startByteCode;
                Token b = getResult();
                if (a.equals("0") && b.equals("1")) {
                    return c;
                } else if (a.equals("1") && b.equals("0")) {
                    return Not.get(c);
                } else if (b.equals("0")) {
                    return And.get(Not.get(c), a);
                } else if (a.equals("0")) {
                    return And.get(c, b);
                } else if (b.equals("1")) {
                    return Or.get(c, a);
                } else if (a.equals("1")) {
                    return And.get(Not.get(c), b);
                }
                return CaseWhen.get(c, b, a);
            }
            if (nextPc != 0) {
                pos = nextPc + startByteCode;
            }
        }
    }

    private void readByteCode() {
        int startPos = pos - startByteCode;
        int opCode = readByte();
        String op;
        endOfMethod = false;
        condition = false;
        nextPc = 0;
        switch (opCode) {
        case 0:
            op = "nop";
            break;
        case 1:
            op = "aconst_null";
            stack.push(Null.INSTANCE);
            break;
        case 2:
            op = "iconst_m1";
            stack.push(ConstantNumber.get("-1"));
            break;
        case 3:
            op = "iconst_0";
            stack.push(ConstantNumber.get("0"));
            break;
        case 4:
            op = "iconst_1";
            stack.push(ConstantNumber.get("1"));
            break;
        case 5:
            op = "iconst_2";
            stack.push(ConstantNumber.get("2"));
            break;
        case 6:
            op = "iconst_3";
            stack.push(ConstantNumber.get("3"));
            break;
        case 7:
            op = "iconst_4";
            stack.push(ConstantNumber.get("4"));
            break;
        case 8:
            op = "iconst_5";
            stack.push(ConstantNumber.get("5"));
            break;
        case 9:
            op = "lconst_0";
            stack.push(ConstantNumber.get("0"));
            break;
        case 10:
            op = "lconst_1";
            stack.push(ConstantNumber.get("1"));
            break;
        case 11:
            op = "fconst_0";
            stack.push(ConstantNumber.get("0.0"));
            break;
        case 12:
            op = "fconst_1";
            stack.push(ConstantNumber.get("1.0"));
            break;
        case 13:
            op = "fconst_2";
            stack.push(ConstantNumber.get("2.0"));
            break;
        case 14:
            op = "dconst_0";
            stack.push(ConstantNumber.get("0.0"));
            break;
        case 15:
            op = "dconst_1";
            stack.push(ConstantNumber.get("1.0"));
            break;
        case 16: {
            int x = (byte) readByte();
            op = "bipush " + x;
            stack.push(ConstantNumber.get(x));
            break;
        }
        case 17: {
            int x = (short) readShort();
            op = "sipush " + x;
            stack.push(ConstantNumber.get(x));
            break;
        }
        case 18: {
            Token s = getConstant(readByte());
            op = "ldc " + s;
            stack.push(s);
            break;
        }
        case 19: {
            Token s = getConstant(readShort());
            op = "ldc_w " + s;
            stack.push(s);
            break;
        }
        case 20: {
            Token s = getConstant(readShort());
            op = "ldc2_w " + s;
            stack.push(s);
            break;
        }
        case 21: {
            int x = readByte();
            op = "iload " + x;
            stack.push(getVariable(x));
            break;
        }
        case 22: {
            int x = readByte();
            op = "lload " + x;
            stack.push(getVariable(x));
            break;
        }
        case 23: {
            int x = readByte();
            op = "fload " + x;
            stack.push(getVariable(x));
            break;
        }
        case 24: {
            int x = readByte();
            op = "dload " + x;
            stack.push(getVariable(x));
            break;
        }
        case 25: {
            int x = readByte();
            op = "aload " + x;
            stack.push(getVariable(x));
            break;
        }
        case 26:
            op = "iload_0";
            stack.push(getVariable(0));
            break;
        case 27:
            op = "iload_1";
            stack.push(getVariable(1));
            break;
        case 28:
            op = "iload_2";
            stack.push(getVariable(2));
            break;
        case 29:
            op = "iload_3";
            stack.push(getVariable(3));
            break;
        case 30:
            op = "lload_0";
            stack.push(getVariable(0));
            break;
        case 31:
            op = "lload_1";
            stack.push(getVariable(1));
            break;
        case 32:
            op = "lload_2";
            stack.push(getVariable(2));
            break;
        case 33:
            op = "lload_3";
            stack.push(getVariable(3));
            break;
        case 34:
            op = "fload_0";
            stack.push(getVariable(0));
            break;
        case 35:
            op = "fload_1";
            stack.push(getVariable(1));
            break;
        case 36:
            op = "fload_2";
            stack.push(getVariable(2));
            break;
        case 37:
            op = "fload_3";
            stack.push(getVariable(3));
            break;
        case 38:
            op = "dload_0";
            stack.push(getVariable(0));
            break;
        case 39:
            op = "dload_1";
            stack.push(getVariable(1));
            break;
        case 40:
            op = "dload_2";
            stack.push(getVariable(2));
            break;
        case 41:
            op = "dload_3";
            stack.push(getVariable(3));
            break;
        case 42:
            op = "aload_0";
            stack.push(getVariable(0));
            break;
        case 43:
            op = "aload_1";
            stack.push(getVariable(1));
            break;
        case 44:
            op = "aload_2";
            stack.push(getVariable(2));
            break;
        case 45:
            op = "aload_3";
            stack.push(getVariable(3));
            break;
        case 46: {
            Token index = stack.pop();
            Token ref = stack.pop();
            op = "iaload";
            stack.push(ArrayGet.get(ref, index));
            break;
        }
        case 47: {
            Token index = stack.pop();
            Token ref = stack.pop();
            op = "laload";
            stack.push(ArrayGet.get(ref, index));
            break;
        }
        case 48: {
            Token index = stack.pop();
            Token ref = stack.pop();
            op = "faload";
            stack.push(ArrayGet.get(ref, index));
            break;
        }
        case 49: {
            Token index = stack.pop();
            Token ref = stack.pop();
            op = "daload";
            stack.push(ArrayGet.get(ref, index));
            break;
        }
        case 50: {
            Token index = stack.pop();
            Token ref = stack.pop();
            op = "aaload";
            stack.push(ArrayGet.get(ref, index));
            break;
        }
        case 51: {
            Token index = stack.pop();
            Token ref = stack.pop();
            op = "baload";
            stack.push(ArrayGet.get(ref, index));
            break;
        }
        case 52: {
            Token index = stack.pop();
            Token ref = stack.pop();
            op = "caload";
            stack.push(ArrayGet.get(ref, index));
            break;
        }
        case 53: {
            Token index = stack.pop();
            Token ref = stack.pop();
            op = "saload";
            stack.push(ArrayGet.get(ref, index));
            break;
        }
        case 54: {
            int var = readByte();
            op = "istore " + var;
            setVariable(var, stack.pop());
            break;
        }
        case 55: {
            int var = readByte();
            op = "lstore " + var;
            setVariable(var, stack.pop());
            break;
        }
        case 56: {
            int var = readByte();
            op = "fstore " + var;
            setVariable(var, stack.pop());
            break;
        }
        case 57: {
            int var = readByte();
            op = "dstore " + var;
            setVariable(var, stack.pop());
            break;
        }
        case 58: {
            int var = readByte();
            op = "astore " + var;
            setVariable(var, stack.pop());
            break;
        }
        case 59:
            op = "istore_0";
            setVariable(0, stack.pop());
            break;
        case 60:
            op = "istore_1";
            setVariable(1, stack.pop());
            break;
        case 61:
            op = "istore_2";
            setVariable(2, stack.pop());
            break;
        case 62:
            op = "istore_3";
            setVariable(3, stack.pop());
            break;
        case 63:
            op = "lstore_0";
            setVariable(0, stack.pop());
            break;
        case 64:
            op = "lstore_1";
            setVariable(1, stack.pop());
            break;
        case 65:
            op = "lstore_2";
            setVariable(2, stack.pop());
            break;
        case 66:
            op = "lstore_3";
            setVariable(3, stack.pop());
            break;
        case 67:
            op = "fstore_0";
            setVariable(0, stack.pop());
            break;
        case 68:
            op = "fstore_1";
            setVariable(1, stack.pop());
            break;
        case 69:
            op = "fstore_2";
            setVariable(2, stack.pop());
            break;
        case 70:
            op = "fstore_3";
            setVariable(3, stack.pop());
            break;
        case 71:
            op = "dstore_0";
            setVariable(0, stack.pop());
            break;
        case 72:
            op = "dstore_1";
            setVariable(1, stack.pop());
            break;
        case 73:
            op = "dstore_2";
            setVariable(2, stack.pop());
            break;
        case 74:
            op = "dstore_3";
            setVariable(3, stack.pop());
            break;
        case 75:
            op = "astore_0";
            setVariable(0, stack.pop());
            break;
        case 76:
            op = "astore_1";
            setVariable(1, stack.pop());
            break;
        case 77:
            op = "astore_2";
            setVariable(2, stack.pop());
            break;
        case 78:
            op = "astore_3";
            setVariable(3, stack.pop());
            break;
        case 79: {
            // String value = stack.pop();
            // String index = stack.pop();
            // String ref = stack.pop();
            op = "iastore";
            // TODO side effect - not supported
            break;
        }
        case 80:
            op = "lastore";
            // TODO side effect - not supported
            break;
        case 81:
            op = "fastore";
            // TODO side effect - not supported
            break;
        case 82:
            op = "dastore";
            // TODO side effect - not supported
            break;
        case 83:
            op = "aastore";
            // TODO side effect - not supported
            break;
        case 84:
            op = "bastore";
            // TODO side effect - not supported
            break;
        case 85:
            op = "castore";
            // TODO side effect - not supported
            break;
        case 86:
            op = "sastore";
            // TODO side effect - not supported
            break;
        case 87:
            op = "pop";
            stack.pop();
            break;
        case 88:
            op = "pop2";
            // TODO currently we don't know the stack types
            stack.pop();
            stack.pop();
            break;
        case 89: {
            op = "dup";
            Token x = stack.pop();
            stack.push(x);
            stack.push(x);
            break;
        }
        case 90: {
            op = "dup_x1";
            Token a = stack.pop();
            Token b = stack.pop();
            stack.push(a);
            stack.push(b);
            stack.push(a);
            break;
        }
        case 91: {
            // TODO currently we don't know the stack types
            op = "dup_x2";
            Token a = stack.pop();
            Token b = stack.pop();
            Token c = stack.pop();
            stack.push(a);
            stack.push(c);
            stack.push(b);
            stack.push(a);
            break;
        }
        case 92: {
            // TODO currently we don't know the stack types
            op = "dup2";
            Token a = stack.pop();
            Token b = stack.pop();
            stack.push(b);
            stack.push(a);
            stack.push(b);
            stack.push(a);
            break;
        }
        case 93: {
            // TODO currently we don't know the stack types
            op = "dup2_x1";
            Token a = stack.pop();
            Token b = stack.pop();
            Token c = stack.pop();
            stack.push(b);
            stack.push(a);
            stack.push(c);
            stack.push(b);
            stack.push(a);
            break;
        }
        case 94: {
            // TODO currently we don't know the stack types
            op = "dup2_x2";
            Token a = stack.pop();
            Token b = stack.pop();
            Token c = stack.pop();
            Token d = stack.pop();
            stack.push(b);
            stack.push(a);
            stack.push(d);
            stack.push(c);
            stack.push(b);
            stack.push(a);
            break;
        }
        case 95: {
            op = "swap";
            Token a = stack.pop();
            Token b = stack.pop();
            stack.push(a);
            stack.push(b);
            break;
        }
        case 96: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "iadd";
            stack.push(Operation.get(a, Operation.Type.ADD, b));
            break;
        }
        case 97: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "ladd";
            stack.push(Operation.get(a, Operation.Type.ADD, b));
            break;
        }
        case 98: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "fadd";
            stack.push(Operation.get(a, Operation.Type.ADD, b));
            break;
        }
        case 99: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "dadd";
            stack.push(Operation.get(a, Operation.Type.ADD, b));
            break;
        }
        case 100: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "isub";
            stack.push(Operation.get(a, Operation.Type.SUBTRACT, b));
            break;
        }
        case 101: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "lsub";
            stack.push(Operation.get(a, Operation.Type.SUBTRACT, b));
            break;
        }
        case 102: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "fsub";
            stack.push(Operation.get(a, Operation.Type.SUBTRACT, b));
            break;
        }
        case 103: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "dsub";
            stack.push(Operation.get(a, Operation.Type.SUBTRACT, b));
            break;
        }
        case 104: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "imul";
            stack.push(Operation.get(a, Operation.Type.MULTIPLY, b));
            break;
        }
        case 105: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "lmul";
            stack.push(Operation.get(a, Operation.Type.MULTIPLY, b));
            break;
        }
        case 106: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "fmul";
            stack.push(Operation.get(a, Operation.Type.MULTIPLY, b));
            break;
        }
        case 107: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "dmul";
            stack.push(Operation.get(a, Operation.Type.MULTIPLY, b));
            break;
        }
        case 108: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "idiv";
            stack.push(Operation.get(a, Operation.Type.DIVIDE, b));
            break;
        }
        case 109: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "ldiv";
            stack.push(Operation.get(a, Operation.Type.DIVIDE, b));
            break;
        }
        case 110: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "fdiv";
            stack.push(Operation.get(a, Operation.Type.DIVIDE, b));
            break;
        }
        case 111: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "ddiv";
            stack.push(Operation.get(a, Operation.Type.DIVIDE, b));
            break;
        }
        case 112: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "irem";
            stack.push(Operation.get(a, Operation.Type.MOD, b));
            break;
        }
        case 113: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "lrem";
            stack.push(Operation.get(a, Operation.Type.MOD, b));
            break;
        }
        case 114: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "frem";
            stack.push(Operation.get(a, Operation.Type.MOD, b));
            break;
        }
        case 115: {
            Token b = stack.pop();
            Token a = stack.pop();
            op = "drem";
            stack.push(Operation.get(a, Operation.Type.MOD, b));
            break;
        }
//        case 116:
//            op = "ineg";
//            break;
//        case 117:
//            op = "lneg";
//            break;
//        case 118:
//            op = "fneg";
//            break;
//        case 119:
//            op = "dneg";
//            break;
//        case 120:
//            op = "ishl";
//            break;
//        case 121:
//            op = "lshl";
//            break;
//        case 122:
//            op = "ishr";
//            break;
//        case 123:
//            op = "lshr";
//            break;
//        case 124:
//            op = "iushr";
//            break;
//        case 125:
//            op = "lushr";
//            break;
//        case 126:
//            op = "iand";
//            break;
//        case 127:
//            op = "land";
//            break;
//        case 128:
//            op = "ior";
//            break;
//        case 129:
//            op = "lor";
//            break;
//        case 130:
//            op = "ixor";
//            break;
//        case 131:
//            op = "lxor";
//            break;
//        case 132: {
//            int var = readByte();
//            int off = (byte) readByte();
//            op = "iinc " + var + " " + off;
//            break;
//        }
//        case 133:
//            op = "i2l";
//            break;
//        case 134:
//            op = "i2f";
//            break;
//        case 135:
//            op = "i2d";
//            break;
//        case 136:
//            op = "l2i";
//            break;
//        case 137:
//            op = "l2f";
//            break;
//        case 138:
//            op = "l2d";
//            break;
//        case 139:
//            op = "f2i";
//            break;
//        case 140:
//            op = "f2l";
//            break;
//        case 141:
//            op = "f2d";
//            break;
//        case 142:
//            op = "d2i";
//            break;
//        case 143:
//            op = "d2l";
//            break;
//        case 144:
//            op = "d2f";
//            break;
//        case 145:
//            op = "i2b";
//            break;
//        case 146:
//            op = "i2c";
//            break;
//        case 147:
//            op = "i2s";
//            break;
        case 148: {
            Token b = stack.pop(), a = stack.pop();
            stack.push(new Function("SIGN", Operation.get(a,
                    Operation.Type.SUBTRACT, b)));
            op = "lcmp";
            break;
        }
//        case 149:
//            op = "fcmpl";
//            break;
//        case 150:
//            op = "fcmpg";
//            break;
//        case 151:
//            op = "dcmpl";
//            break;
//        case 152:
//            op = "dcmpg";
//            break;
        case 153:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push(Operation.get(stack.pop(), Operation.Type.EQUALS,
                    ConstantNumber.get(0)));
            op = "ifeq " + nextPc;
            break;
        case 154:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push(Operation.get(stack.pop(), Operation.Type.NOT_EQUALS,
                    ConstantNumber.get(0)));
            op = "ifne " + nextPc;
            break;
        case 155:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push(Operation.get(stack.pop(), Operation.Type.SMALLER,
                    ConstantNumber.get(0)));
            op = "iflt " + nextPc;
            break;
        case 156:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push(Operation.get(stack.pop(), Operation.Type.BIGGER_EQUALS,
                    ConstantNumber.get(0)));
            op = "ifge " + nextPc;
            break;
        case 157:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push(Operation.get(stack.pop(), Operation.Type.BIGGER,
                    ConstantNumber.get(0)));
            op = "ifgt " + nextPc;
            break;
        case 158:
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            stack.push(Operation.get(stack.pop(),
                    Operation.Type.SMALLER_EQUALS, ConstantNumber.get(0)));
            op = "ifle " + nextPc;
            break;
        case 159: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            Token b = stack.pop(), a = stack.pop();
            stack.push(Operation.get(a, Operation.Type.EQUALS, b));
            op = "if_icmpeq " + nextPc;
            break;
        }
        case 160: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            Token b = stack.pop(), a = stack.pop();
            stack.push(Operation.get(a, Operation.Type.NOT_EQUALS, b));
            op = "if_icmpne " + nextPc;
            break;
        }
        case 161: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            Token b = stack.pop(), a = stack.pop();
            stack.push(Operation.get(a, Operation.Type.SMALLER, b));
            op = "if_icmplt " + nextPc;
            break;
        }
        case 162: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            Token b = stack.pop(), a = stack.pop();
            stack.push(Operation.get(a, Operation.Type.BIGGER_EQUALS, b));
            op = "if_icmpge " + nextPc;
            break;
        }
        case 163: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            Token b = stack.pop(), a = stack.pop();
            stack.push(Operation.get(a, Operation.Type.BIGGER, b));
            op = "if_icmpgt " + nextPc;
            break;
        }
        case 164: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            Token b = stack.pop(), a = stack.pop();
            stack.push(Operation.get(a, Operation.Type.SMALLER_EQUALS, b));
            op = "if_icmple " + nextPc;
            break;
        }
        case 165: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            Token b = stack.pop(), a = stack.pop();
            stack.push(Operation.get(a, Operation.Type.EQUALS, b));
            op = "if_acmpeq " + nextPc;
            break;
        }
        case 166: {
            condition = true;
            nextPc = getAbsolutePos(pos, readShort());
            Token b = stack.pop(), a = stack.pop();
            stack.push(Operation.get(a, Operation.Type.NOT_EQUALS, b));
            op = "if_acmpne " + nextPc;
            break;
        }
        case 167:
            nextPc = getAbsolutePos(pos, readShort());
            op = "goto " + nextPc;
            break;
//        case 168:
//            // TODO not supported yet
//            op = "jsr " + getAbsolutePos(pos, readShort());
//            break;
//        case 169:
//            // TODO not supported yet
//            op = "ret " + readByte();
//            break;
//        case 170: {
//            int start = pos;
//            pos += 4 - ((pos - startByteCode) & 3);
//            int def = readInt();
//            int low = readInt(), high = readInt();
//            int n = high - low + 1;
//            op = "tableswitch default:" + getAbsolutePos(start, def);
//            StringBuilder buff = new StringBuilder();
//            for (int i = 0; i < n; i++) {
//                buff.append(' ').append(low++).
//                    append(":").
//                    append(getAbsolutePos(start, readInt()));
//            }
//            op += buff.toString();
//            // pos += n * 4;
//            break;
//        }
//        case 171: {
//            int start = pos;
//            pos += 4 - ((pos - startByteCode) & 3);
//            int def = readInt();
//            int n = readInt();
//            op = "lookupswitch default:" + getAbsolutePos(start, def);
//            StringBuilder buff = new StringBuilder();
//            for (int i = 0; i < n; i++) {
//                buff.append(' ').
//                    append(readInt()).
//                    append(":").
//                    append(getAbsolutePos(start, readInt()));
//            }
//            op += buff.toString();
//            // pos += n * 8;
//            break;
//        }
        case 172:
            op = "ireturn";
            endOfMethod = true;
            break;
        case 173:
            op = "lreturn";
            endOfMethod = true;
            break;
        case 174:
            op = "freturn";
            endOfMethod = true;
            break;
        case 175:
            op = "dreturn";
            endOfMethod = true;
            break;
        case 176:
            op = "areturn";
            endOfMethod = true;
            break;
        case 177:
            op = "return";
            // no value returned
            stack.push(null);
            endOfMethod = true;
            break;
//        case 178:
//            op = "getstatic " + getField(readShort());
//            break;
//        case 179:
//            op = "putstatic " + getField(readShort());
//            break;
        case 180: {
            String field = getField(readShort());
            Token p = stack.pop();
            String s = p
                    + "."
                    + field.substring(field.lastIndexOf('.') + 1,
                            field.indexOf(' '));
            if (s.startsWith("this.")) {
                s = s.substring(5);
            }
            stack.push(Variable.get(s, fieldMap.get(s)));
            op = "getfield " + field;
            break;
        }
//        case 181:
//            op = "putfield " + getField(readShort());
//            break;
        case 182: {
            String method = getMethod(readShort());
            op = "invokevirtual " + method;
            if (method.equals("java/lang/String.equals (Ljava/lang/Object;)Z")) {
                Token a = stack.pop();
                Token b = stack.pop();
                stack.push(Operation.get(a, Operation.Type.EQUALS, b));
            } else if (method.equals("java/lang/Integer.intValue ()I")) {
                // ignore
            } else if (method.equals("java/lang/Long.longValue ()J")) {
                // ignore
            }
            break;
        }
        case 183: {
            String method = getMethod(readShort());
            op = "invokespecial " + method;
            break;
        }
        case 184:
            op = "invokestatic " + getMethod(readShort());
            break;
//        case 185: {
//            int methodRef = readShort();
//            readByte();
//            readByte();
//            op = "invokeinterface " + getMethod(methodRef);
//            break;
//        }
        case 187: {
            String className = constantPool[constantPool[readShort()]
                    .intValue()].toString();
            op = "new " + className;
            break;
        }
//        case 188:
//            op = "newarray " + readByte();
//            break;
//        case 189:
//            op = "anewarray " + cpString[readShort()];
//            break;
//        case 190:
//            op = "arraylength";
//            break;
//        case 191:
//            op = "athrow";
//            break;
//        case 192:
//            op = "checkcast " + cpString[readShort()];
//            break;
//        case 193:
//            op = "instanceof " + cpString[readShort()];
//            break;
//        case 194:
//            op = "monitorenter";
//            break;
//        case 195:
//            op = "monitorexit";
//            break;
//        case 196: {
//            opCode = readByte();
//            switch (opCode) {
//            case 21:
//                op = "wide iload " + readShort();
//                break;
//            case 22:
//                op = "wide lload " + readShort();
//                break;
//            case 23:
//                op = "wide fload " + readShort();
//                break;
//            case 24:
//                op = "wide dload " + readShort();
//                break;
//            case 25:
//                op = "wide aload " + readShort();
//                break;
//            case 54:
//                op = "wide istore " + readShort();
//                break;
//            case 55:
//                op = "wide lstore " + readShort();
//                break;
//            case 56:
//                op = "wide fstore " + readShort();
//                break;
//            case 57:
//                op = "wide dstore " + readShort();
//                break;
//            case 58:
//                op = "wide astore " + readShort();
//                break;
//            case 132: {
//                int var = readShort();
//                int off = (short) readShort();
//                op = "wide iinc " + var + " " + off;
//                break;
//            }
//            case 169:
//                op = "wide ret " + readShort();
//                break;
//            default:
//                throw new RuntimeException(
//                        "Unsupported wide opCode " + opCode);
//            }
//            break;
//        }
//        case 197:
//            op = "multianewarray " + cpString[readShort()] + " " + readByte();
//            break;
//        case 198: {
//            condition = true;
//            nextPc = getAbsolutePos(pos, readShort());
//            Token a = stack.pop();
//            stack.push("(" + a + " IS NULL)");
//            op = "ifnull " + nextPc;
//            break;
//        }
//        case 199: {
//            condition = true;
//            nextPc = getAbsolutePos(pos, readShort());
//            Token a = stack.pop();
//            stack.push("(" + a + " IS NOT NULL)");
//            op = "ifnonnull " + nextPc;
//            break;
//        }
        case 200:
            op = "goto_w " + getAbsolutePos(pos, readInt());
            break;
        case 201:
            op = "jsr_w " + getAbsolutePos(pos, readInt());
            break;
        default:
            throw new RuntimeException("Unsupported opCode " + opCode);
        }
        debug("    " + startPos + ": " + op);
    }

    private void setVariable(int x, Token value) {
        while (x >= variables.size()) {
            variables.add(Variable.get("p" + variables.size(), null));
        }
        variables.set(x, value);
    }

    private Token getVariable(int x) {
        if (x == 0) {
            return Variable.THIS;
        }
        while (x >= variables.size()) {
            variables.add(Variable.get("p" + variables.size(), null));
        }
        return variables.get(x);
    }

    private String getField(int fieldRef) {
        int field = constantPool[fieldRef].intValue();
        int classIndex = field >>> 16;
        int nameAndType = constantPool[field & 0xffff].intValue();
        String className = constantPool[constantPool[classIndex].intValue()]
                + "." + constantPool[nameAndType >>> 16] + " "
                + constantPool[nameAndType & 0xffff];
        return className;
    }

    private String getMethod(int methodRef) {
        int method = constantPool[methodRef].intValue();
        int classIndex = method >>> 16;
        int nameAndType = constantPool[method & 0xffff].intValue();
        String className = constantPool[constantPool[classIndex].intValue()]
                + "." + constantPool[nameAndType >>> 16] + " "
                + constantPool[nameAndType & 0xffff];
        return className;
    }

    private Constant getConstant(int constantRef) {
        Constant c = constantPool[constantRef];
        switch (c.getType()) {
        case INT:
        case FLOAT:
        case DOUBLE:
        case LONG:
            return c;
        case STRING_REF:
            return constantPool[c.intValue()];
        default:
            throw new RuntimeException("Not a constant: " + constantRef);
        }
    }

    private String readString() {
        int size = readShort();
        byte[] buff = data;
        int p = pos, end = p + size;
        char[] chars = new char[size];
        int j = 0;
        for (; p < end; j++) {
            int x = buff[p++] & 0xff;
            if (x < 0x80) {
                chars[j] = (char) x;
            } else if (x >= 0xe0) {
                chars[j] = (char) (((x & 0xf) << 12)
                        + ((buff[p++] & 0x3f) << 6) + (buff[p++] & 0x3f));
            } else {
                chars[j] = (char) (((x & 0x1f) << 6) + (buff[p++] & 0x3f));
            }
        }
        pos = p;
        return new String(chars, 0, j);
    }

    private int getAbsolutePos(int start, int offset) {
        return start - startByteCode - 1 + (short) offset;
    }

    private int readByte() {
        return data[pos++] & 0xff;
    }

    private int readShort() {
        byte[] buff = data;
        return ((buff[pos++] & 0xff) << 8) + (buff[pos++] & 0xff);
    }

    private int readInt() {
        byte[] buff = data;
        return (buff[pos++] << 24) + ((buff[pos++] & 0xff) << 16)
                + ((buff[pos++] & 0xff) << 8) + (buff[pos++] & 0xff);
    }

    private long readLong() {
        return ((long) (readInt()) << 32) + (readInt() & 0xffffffffL);
    }

}
