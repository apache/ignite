/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;
import org.h2.util.SourceCompiler;

/**
 * A code generator for class proxies.
 */
public class ProxyCodeGenerator {

    private static SourceCompiler compiler = new SourceCompiler();
    private static HashMap<Class<?>, Class<?>> proxyMap = new HashMap<>();

    private final TreeSet<String> imports = new TreeSet<>();
    private final TreeMap<String, Method> methods = new TreeMap<>();
    private String packageName;
    private String className;
    private Class<?> extendsClass;
    private Constructor<?> constructor;

    /**
     * Check whether there is already a proxy class generated.
     *
     * @param c the class
     * @return true if yes
     */
    public static boolean isGenerated(Class<?> c) {
        return proxyMap.containsKey(c);
    }

    /**
     * Generate a proxy class. The returned class extends the given class.
     *
     * @param c the class to extend
     * @return the proxy class
     */
    public static Class<?> getClassProxy(Class<?> c) throws ClassNotFoundException {
        Class<?> p = proxyMap.get(c);
        if (p != null) {
            return p;
        }
        // TODO how to extend a class with private constructor
        // TODO call right constructor
        // TODO use the right package
        ProxyCodeGenerator cg = new ProxyCodeGenerator();
        cg.setPackageName("bytecode");
        cg.generateClassProxy(c);
        StringWriter sw = new StringWriter();
        cg.write(new PrintWriter(sw));
        String code = sw.toString();
        String proxy = "bytecode."+ c.getSimpleName() + "Proxy";
        compiler.setJavaSystemCompiler(false);
        compiler.setSource(proxy, code);
        // System.out.println(code);
        Class<?> px = compiler.getClass(proxy);
        proxyMap.put(c, px);
        return px;
    }

    private void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    /**
     * Generate a class that implements all static methods of the given class,
     * but as non-static.
     *
     * @param clazz the class to extend
     */
    void generateStaticProxy(Class<?> clazz) {
        imports.clear();
        addImport(InvocationHandler.class);
        addImport(Method.class);
        addImport(clazz);
        className = getClassName(clazz) + "Proxy";
        for (Method m : clazz.getDeclaredMethods()) {
            if (Modifier.isStatic(m.getModifiers())) {
                if (!Modifier.isPrivate(m.getModifiers())) {
                    addMethod(m);
                }
            }
        }
    }

    private void generateClassProxy(Class<?> clazz) {
        imports.clear();
        addImport(InvocationHandler.class);
        addImport(Method.class);
        addImport(clazz);
        className = getClassName(clazz) + "Proxy";
        extendsClass = clazz;
        int doNotOverride = Modifier.FINAL | Modifier.STATIC |
                Modifier.PRIVATE | Modifier.ABSTRACT | Modifier.VOLATILE;
        Class<?> dc = clazz;
        while (dc != null) {
            addImport(dc);
            for (Method m : dc.getDeclaredMethods()) {
                if ((m.getModifiers() & doNotOverride) == 0) {
                    addMethod(m);
                }
            }
            dc = dc.getSuperclass();
        }
        for (Constructor<?> c : clazz.getDeclaredConstructors()) {
            if (Modifier.isPrivate(c.getModifiers())) {
                continue;
            }
            if (constructor == null) {
                constructor = c;
            } else if (c.getParameterTypes().length <
                    constructor.getParameterTypes().length) {
                constructor = c;
            }
        }
    }

    private void addMethod(Method m) {
        if (methods.containsKey(getMethodName(m))) {
            // already declared in a subclass
            return;
        }
        addImport(m.getReturnType());
        for (Class<?> c : m.getParameterTypes()) {
            addImport(c);
        }
        for (Class<?> c : m.getExceptionTypes()) {
            addImport(c);
        }
        methods.put(getMethodName(m), m);
    }

    private static String getMethodName(Method m) {
        StringBuilder buff = new StringBuilder();
        buff.append(m.getReturnType()).append(' ');
        buff.append(m.getName());
        for (Class<?> p : m.getParameterTypes()) {
            buff.append(' ');
            buff.append(p.getName());
        }
        return buff.toString();
    }

    private void addImport(Class<?> c) {
        while (c.isArray()) {
            c = c.getComponentType();
        }
        if (!c.isPrimitive()) {
            if (!"java.lang".equals(c.getPackage().getName())) {
                imports.add(c.getName());
            }
        }
    }

    private static String getClassName(Class<?> c) {
        return getClassName(c, false);
    }

    private static String getClassName(Class<?> c, boolean varArg) {
        if (varArg) {
            c = c.getComponentType();
        }
        String s = c.getSimpleName();
        while (true) {
            c = c.getEnclosingClass();
            if (c == null) {
                break;
            }
            s = c.getSimpleName() + "." + s;
        }
        if (varArg) {
            return s + "...";
        }
        return s;
    }

    private void write(PrintWriter writer) {
        if (packageName != null) {
            writer.println("package " + packageName + ";");
        }
        for (String imp : imports) {
            writer.println("import " + imp + ";");
        }
        writer.print("public class " + className);
        if (extendsClass != null) {
            writer.print(" extends " + getClassName(extendsClass));
        }
        writer.println(" {");
        writer.println("    private final InvocationHandler ih;");
        writer.println("    public " + className + "() {");
        writer.println("        this(new InvocationHandler() {");
        writer.println("            public Object invoke(Object proxy,");
        writer.println("                    Method method, Object[] args) " +
                "throws Throwable {");
        writer.println("                return method.invoke(proxy, args);");
        writer.println("            }});");
        writer.println("    }");
        writer.println("    public " + className + "(InvocationHandler ih) {");
        if (constructor != null) {
            writer.print("        super(");
            int i = 0;
            for (Class<?> p : constructor.getParameterTypes()) {
                if (i > 0) {
                    writer.print(", ");
                }
                if (p.isPrimitive()) {
                    if (p == boolean.class) {
                        writer.print("false");
                    } else if (p == byte.class) {
                        writer.print("(byte) 0");
                    } else if (p == char.class) {
                        writer.print("(char) 0");
                    } else if (p == short.class) {
                        writer.print("(short) 0");
                    } else if (p == int.class) {
                        writer.print("0");
                    } else if (p == long.class) {
                        writer.print("0L");
                    } else if (p == float.class) {
                        writer.print("0F");
                    } else if (p == double.class) {
                        writer.print("0D");
                    }
                } else {
                    writer.print("null");
                }
                i++;
            }
            writer.println(");");
        }
        writer.println("        this.ih = ih;");
        writer.println("    }");
        writer.println("    @SuppressWarnings(\"unchecked\")");
        writer.println("    private static <T extends RuntimeException> " +
                "T convertException(Throwable e) {");
        writer.println("        if (e instanceof Error) {");
        writer.println("            throw (Error) e;");
        writer.println("        }");
        writer.println("        return (T) e;");
        writer.println("    }");
        for (Method m : methods.values()) {
            Class<?> retClass = m.getReturnType();
            writer.print("    ");
            if (Modifier.isProtected(m.getModifiers())) {
                // 'public' would also work
                writer.print("protected ");
            } else {
                writer.print("public ");
            }
            writer.print(getClassName(retClass) +
                " " + m.getName() + "(");
            Class<?>[] pc = m.getParameterTypes();
            for (int i = 0; i < pc.length; i++) {
                Class<?> p = pc[i];
                if (i > 0) {
                    writer.print(", ");
                }
                boolean varArg = i == pc.length - 1 && m.isVarArgs();
                writer.print(getClassName(p, varArg) + " p" + i);
            }
            writer.print(")");
            Class<?>[] ec = m.getExceptionTypes();
            writer.print(" throws RuntimeException");
            if (ec.length > 0) {
                for (Class<?> e : ec) {
                    writer.print(", ");
                    writer.print(getClassName(e));
                }
            }
            writer.println(" {");
            writer.println("        try {");
            writer.print("            ");
            if (retClass != void.class) {
                writer.print("return (");
                if (retClass == boolean.class) {
                    writer.print("Boolean");
                } else if (retClass == byte.class) {
                    writer.print("Byte");
                } else if (retClass == char.class) {
                    writer.print("Character");
                } else if (retClass == short.class) {
                    writer.print("Short");
                } else if (retClass == int.class) {
                    writer.print("Integer");
                } else if (retClass == long.class) {
                    writer.print("Long");
                } else if (retClass == float.class) {
                    writer.print("Float");
                } else if (retClass == double.class) {
                    writer.print("Double");
                } else {
                    writer.print(getClassName(retClass));
                }
                writer.print(") ");
            }
            writer.print("ih.invoke(this, ");
            writer.println(getClassName(m.getDeclaringClass()) +
                    ".class.getDeclaredMethod(\"" + m.getName() +
                    "\",");
            writer.print("                new Class[] {");
            int i = 0;
            for (Class<?> p : m.getParameterTypes()) {
                if (i > 0) {
                    writer.print(", ");
                }
                writer.print(getClassName(p) + ".class");
                i++;
            }
            writer.println("}),");
            writer.print("                new Object[] {");
            for (i = 0; i < m.getParameterTypes().length; i++) {
                if (i > 0) {
                    writer.print(", ");
                }
                writer.print("p" + i);
            }
            writer.println("});");
            writer.println("        } catch (Throwable e) {");
            writer.println("            throw convertException(e);");
            writer.println("        }");
            writer.println("    }");
        }
        writer.println("}");
        writer.flush();
    }

    /**
     * Format a method call, including arguments, for an exception message.
     *
     * @param m the method
     * @param args the arguments
     * @return the formatted string
     */
    public static String formatMethodCall(Method m, Object... args) {
        StringBuilder buff = new StringBuilder();
        buff.append(m.getName()).append('(');
        for (int i = 0; i < args.length; i++) {
            Object a = args[i];
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(a == null ? "null" : a.toString());
        }
        buff.append(")");
        return buff.toString();
    }

}
