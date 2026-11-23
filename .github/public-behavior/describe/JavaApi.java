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

import java.lang.annotation.Annotation;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Describes the public and protected declarations of the packages published in javadoc, in the modules of {@code ignite-bom}.
 * Types generated during the build have no source file and are skipped: they serialize node-to-node messages, which are
 * outside the public contract.
 */
class JavaApi implements Collector {
    /** */
    private static final String AREA = "Java API";

    /** Packages of Ignite types, public or internal. */
    private static final List<String> IGNITE_PACKAGES = List.of("org.apache.ignite.", "org.jsr166.");

    /** {@inheritDoc} */
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        int types = 0;

        for (Class<?> cls : tree.classes("", true, out)) {
            try {
                if (apiType(tree, cls) && !tree.where(cls, null, -1, false).isEmpty()) {
                    javaType(tree, out, cls);

                    types++;
                }
            }
            catch (Throwable e) {
                out.error("Failed to describe " + cls.getName() + ": " + e);
            }
        }

        if (types == 0)
            out.error("No public types found in " + tree.classDirs());
    }

    /** @return {@code True} if users can reference the type. */
    private static boolean apiType(BuiltTree tree, Class<?> cls) {
        if (cls.isSynthetic() || cls.isAnonymousClass() || cls.isLocalClass() || !tree.publicPackage(cls.getPackageName()))
            return false;

        Class<?> owner = cls.getDeclaringClass();

        return owner == null ? Modifier.isPublic(cls.getModifiers()) : accessible(cls.getModifiers(), owner) && apiType(tree, owner);
    }

    /** @return {@code True} if users can reach a member with these modifiers. */
    private static boolean accessible(int mod, Class<?> owner) {
        return Modifier.isPublic(mod) || Modifier.isProtected(mod) && !Modifier.isFinal(owner.getModifiers()) && !owner.isEnum();
    }

    /** @return {@code True} if the type belongs to Ignite but not to its public API. */
    private static boolean internalType(BuiltTree tree, Class<?> cls) {
        return IGNITE_PACKAGES.stream().anyMatch(cls.getName()::startsWith) && !apiType(tree, cls);
    }

    /** @return {@code True} if the element or a type that encloses it has the annotation. */
    private static boolean marked(AnnotatedElement e, Class<?> owner, Class<? extends Annotation> ann) {
        if (e.isAnnotationPresent(ann))
            return true;

        for (Class<?> c = owner; c != null; c = c.getDeclaringClass()) {
            if (c.isAnnotationPresent(ann) || c.getPackage() != null && c.getPackage().isAnnotationPresent(ann))
                return true;
        }

        return false;
    }

    /**
     * @return Names of the type parameters of the class and of the classes that enclose it, by position: renaming a type
     *      parameter does not change what users can write.
     */
    private static Map<TypeVariable<?>, String> typeVars(Class<?> cls) {
        Map<TypeVariable<?>, String> res = new HashMap<>();
        int depth = 0;

        for (Class<?> c = cls; c != null; c = c.getDeclaringClass(), depth++) {
            TypeVariable<?>[] vars = c.getTypeParameters();

            for (int i = 0; i < vars.length; i++)
                res.put(vars[i], "#" + (depth == 0 ? "" : depth + ".") + (i + 1));
        }

        return res;
    }

    /** @return Type parameters of a method or constructor named by position, added to those of its class. */
    private static Map<TypeVariable<?>, String> typeVars(Executable e, Map<TypeVariable<?>, String> classVars) {
        Map<TypeVariable<?>, String> res = new HashMap<>(classVars);
        TypeVariable<?>[] vars = e.getTypeParameters();

        for (int i = 0; i < vars.length; i++)
            res.put(vars[i], "#m" + (i + 1));

        return res;
    }

    /** @return Type as users write it, with type parameters named by position. */
    private static String typeText(Type t, Map<TypeVariable<?>, String> vars) {
        if (t instanceof Class)
            return ((Class<?>)t).isArray() ? typeText(((Class<?>)t).getComponentType(), vars) + "[]" : t.getTypeName();

        if (t instanceof ParameterizedType) {
            ParameterizedType p = (ParameterizedType)t;

            return typeText(p.getRawType(), vars)
                + Arrays.stream(p.getActualTypeArguments()).map(a -> typeText(a, vars)).collect(Collectors.joining(", ", "<", ">"));
        }

        if (t instanceof TypeVariable)
            return vars.getOrDefault(t, ((TypeVariable<?>)t).getName());

        if (t instanceof WildcardType) {
            WildcardType w = (WildcardType)t;

            if (w.getLowerBounds().length > 0)
                return "? super " + typeText(w.getLowerBounds()[0], vars);

            return w.getUpperBounds()[0] == Object.class ? "?" : "? extends " + typeText(w.getUpperBounds()[0], vars);
        }

        if (t instanceof GenericArrayType)
            return typeText(((GenericArrayType)t).getGenericComponentType(), vars) + "[]";

        return t.getTypeName();
    }

    /** Describes a public type and the members that users can call or extend. */
    private static void javaType(BuiltTree tree, Declarations out, Class<?> cls) {
        String name = cls.getCanonicalName();
        boolean deprecated = marked(cls, cls.getDeclaringClass(), Deprecated.class);
        boolean experimental = marked(cls, cls.getDeclaringClass(), IgniteExperimental.class);
        Map<TypeVariable<?>, String> vars = typeVars(cls);

        Set<String> supers = new TreeSet<>();

        collectSupertypes(tree, cls, vars, supers);

        String kind = cls.isAnnotation() ? "@interface" : cls.isInterface() ? "interface" : cls.isEnum() ? "enum" : "class";
        int mask = Modifier.PUBLIC | Modifier.PROTECTED | Modifier.STATIC
            | (cls.isInterface() || cls.isEnum() ? 0 : Modifier.FINAL | Modifier.ABSTRACT);
        String typeDeprecation = tree.deprecationWhere(cls, null, -1, deprecated);

        out.add(AREA, name, markers(cls) + modifiers(cls.getModifiers() & mask) + kind + ' ' + cls.getSimpleName()
            + typeParams(cls.getTypeParameters(), vars) + (supers.isEmpty() ? "" : " : " + String.join(", ", supers))
            + annotationUse(cls), Declarations.flags(deprecated, experimental), tree.where(cls, null, -1, false), typeDeprecation, "");

        Map<String, Member> members = new HashMap<>();

        describeMembers(cls, cls, vars, members);

        Map<Class<?>, Map<TypeVariable<?>, String>> internalSupers = new LinkedHashMap<>();

        collectInternalSupertypes(tree, cls, vars, internalSupers);

        internalSupers.forEach((sup, supVars) -> describeMembers(sup, cls, supVars, members));

        members.forEach((key, m) -> {
            boolean own = m.elem.isAnnotationPresent(Deprecated.class);
            boolean exp = experimental || marked(m.elem, m.owner, IgniteExperimental.class);

            out.add(AREA, name + '#' + key, m.text, Declarations.flags(deprecated || own, exp), tree.where(m.owner, m.name, m.params, false),
                own ? tree.where(m.owner, m.name, m.params, true) : typeDeprecation, "");
        });
    }

    /** @return Where an annotation type may be used and how long it is kept, or an empty string for other types. */
    private static String annotationUse(Class<?> cls) {
        if (!cls.isAnnotation())
            return "";

        Target target = cls.getAnnotation(Target.class);
        Retention retention = cls.getAnnotation(Retention.class);

        return " @Target" + (target == null ? "(default)"
            : Arrays.stream(target.value()).map(Enum::name).sorted().collect(Collectors.joining(", ", "{", "}")))
            + " @Retention(" + (retention == null ? "CLASS" : retention.value()) + ')'
            + (cls.isAnnotationPresent(Inherited.class) ? " @Inherited" : "")
            + (cls.isAnnotationPresent(Repeatable.class) ? " @Repeatable" : "");
    }

    /** Member of a public type. */
    private static class Member {
        /** */
        final AnnotatedElement elem;

        /** Class that declares the member. */
        final Class<?> owner;

        /** Name in the source. */
        final String name;

        /** Number of parameters of a method or constructor, or -1 for a field. */
        final int params;

        /** Description. */
        final String text;

        /** */
        Member(AnnotatedElement elem, Class<?> owner, String name, int params, String text) {
            this.elem = elem;
            this.owner = owner;
            this.name = name;
            this.params = params;
            this.text = text;
        }
    }

    /** Collects public supertypes, looking through Ignite internal ones. */
    private static void collectSupertypes(BuiltTree tree, Class<?> cls, Map<TypeVariable<?>, String> vars, Set<String> res) {
        List<Type> direct = new ArrayList<>(Arrays.asList(cls.getGenericInterfaces()));

        if (cls.getGenericSuperclass() != null && cls.getSuperclass() != Object.class && !cls.isEnum())
            direct.add(cls.getGenericSuperclass());

        for (Type t : direct) {
            Class<?> raw = rawClass(t);

            if (internalType(tree, raw))
                collectSupertypes(tree, raw, passedVars(t, vars), res);
            else if (raw != Annotation.class)
                res.add(typeText(t, vars));
        }
    }

    /**
     * Collects Ignite internal supertypes whose members users reach through the public type, each with the type arguments
     * that the public type passes to it: renaming a type parameter of an internal class changes nothing for users.
     */
    private static void collectInternalSupertypes(BuiltTree tree, Class<?> cls, Map<TypeVariable<?>, String> vars,
        Map<Class<?>, Map<TypeVariable<?>, String>> res) {
        List<Type> direct = new ArrayList<>(Arrays.asList(cls.getGenericInterfaces()));

        if (cls.getGenericSuperclass() != null)
            direct.add(cls.getGenericSuperclass());

        for (Type t : direct) {
            Class<?> sup = rawClass(t);

            if (internalType(tree, sup) && !res.containsKey(sup)) {
                Map<TypeVariable<?>, String> supVars = passedVars(t, vars);

                res.put(sup, supVars);
                collectInternalSupertypes(tree, sup, supVars, res);
            }
        }
    }

    /** @return Class of a class or parameterized type. */
    private static Class<?> rawClass(Type t) {
        return t instanceof ParameterizedType ? (Class<?>)((ParameterizedType)t).getRawType() : (Class<?>)t;
    }

    /** @return Type parameters of a supertype mapped to the type arguments that a subtype passes in {@code t}. */
    private static Map<TypeVariable<?>, String> passedVars(Type t, Map<TypeVariable<?>, String> vars) {
        TypeVariable<?>[] params = rawClass(t).getTypeParameters();
        Type[] args = t instanceof ParameterizedType ? ((ParameterizedType)t).getActualTypeArguments() : new Type[0];
        Map<TypeVariable<?>, String> res = new HashMap<>();

        for (int i = 0; i < params.length; i++)
            res.put(params[i], i < args.length ? typeText(args[i], vars) : params[i].getName());

        return res;
    }

    /**
     * Describes the members of {@code src} as members of the public type {@code api}, keeping members described earlier.
     * A static method of an interface belongs to that interface only: Java does not inherit it.
     */
    private static void describeMembers(Class<?> src, Class<?> api, Map<TypeVariable<?>, String> vars, Map<String, Member> res) {
        for (Constructor<?> c : src == api ? src.getDeclaredConstructors() : new Constructor<?>[0]) {
            if (!c.isSynthetic() && accessible(c.getModifiers(), api)) {
                Map<TypeVariable<?>, String> v = typeVars(c, vars);

                res.putIfAbsent("<init>" + params(c), new Member(c, src, api.getSimpleName(), c.getParameterCount(),
                    markers(c) + modifiers(c.getModifiers()) + typeParams(c.getTypeParameters(), v) + api.getSimpleName() + signature(c, v)));
            }
        }

        for (Method m : src.getDeclaredMethods()) {
            if (m.isSynthetic() || m.isBridge() || !accessible(m.getModifiers(), api)
                || src != api && src.isInterface() && Modifier.isStatic(m.getModifiers()))
                continue;

            Map<TypeVariable<?>, String> v = typeVars(m, vars);
            String dflt = m.getDefaultValue() == null ? "" : " default " + Declarations.value(m.getDefaultValue());

            res.putIfAbsent(m.getName() + params(m), new Member(m, src, m.getName(), m.getParameterCount(), markers(m)
                + modifiers(m.getModifiers()) + (m.isDefault() ? "default " : "") + typeParams(m.getTypeParameters(), v)
                + typeText(m.getGenericReturnType(), v) + ' ' + m.getName() + signature(m, v) + dflt));
        }

        for (Field f : src.getDeclaredFields()) {
            if (f.isSynthetic() || !accessible(f.getModifiers(), api))
                continue;

            res.putIfAbsent(f.getName(), new Member(f, src, f.getName(), -1, markers(f) + modifiers(f.getModifiers())
                + typeText(f.getGenericType(), vars) + ' ' + f.getName() + constantValue(f)));
        }
    }

    /** @return Erased parameter types that identify an overload. */
    private static String params(Executable e) {
        return Arrays.stream(e.getParameterTypes()).map(Class::getTypeName).collect(Collectors.joining(",", "(", ")"));
    }

    /** @return Generic parameter and exception types. */
    private static String signature(Executable e, Map<TypeVariable<?>, String> vars) {
        Type[] params = e.getGenericParameterTypes();
        List<String> names = new ArrayList<>();

        for (int i = 0; i < params.length; i++) {
            String n = typeText(params[i], vars);

            names.add(e.isVarArgs() && i == params.length - 1 ? n.substring(0, n.length() - 2) + "..." : n);
        }

        Type[] exceptions = e.getGenericExceptionTypes();

        return '(' + String.join(", ", names) + ')' + (exceptions.length == 0 ? "" :
            Arrays.stream(exceptions).map(t -> typeText(t, vars)).sorted().collect(Collectors.joining(", ", " throws ", "")));
    }

    /** @return Type parameters with their bounds. */
    private static String typeParams(TypeVariable<?>[] params, Map<TypeVariable<?>, String> vars) {
        if (params.length == 0)
            return "";

        return Arrays.stream(params).map(p -> {
            List<String> bounds = Arrays.stream(p.getBounds()).filter(b -> b != Object.class).map(b -> typeText(b, vars))
                .collect(Collectors.toList());

            return vars.get(p) + (bounds.isEmpty() ? "" : " extends " + String.join(" & ", bounds));
        }).collect(Collectors.joining(", ", "<", "> "));
    }

    /** @return Annotations that change what users may rely on. */
    private static String markers(AnnotatedElement e) {
        return (e.isAnnotationPresent(Deprecated.class) ? "@Deprecated " : "")
            + (e.isAnnotationPresent(IgniteExperimental.class) ? "@IgniteExperimental " : "");
    }

    /** */
    private static String modifiers(int mod) {
        String s = Modifier.toString(mod & (Modifier.PUBLIC | Modifier.PROTECTED | Modifier.STATIC | Modifier.FINAL | Modifier.ABSTRACT));

        return s.isEmpty() ? "" : s + ' ';
    }

    /** @return Enum ordinal or the value of a constant that compilers copy into user code. */
    private static String constantValue(Field f) {
        if (f.isEnumConstant())
            return " #" + ((Enum<?>)Declarations.staticValue(f)).ordinal();

        int mod = f.getModifiers();

        if (!Modifier.isStatic(mod) || !Modifier.isFinal(mod) || !(f.getType().isPrimitive() || f.getType() == String.class))
            return "";

        return " = " + Declarations.value(Declarations.staticValue(f));
    }
}
