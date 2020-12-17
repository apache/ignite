package org.apache.ignite.configuration.processor.internal;

/**
 * Type names for testing.
 */
public class Types {
    /** Java lang package name. */
    private static final String PKG_JAVA_LANG = "java.lang.";

    /** Integer. */
    public static final String INT = PKG_JAVA_LANG + "Integer";

    /** Long. */
    public static final String LONG = PKG_JAVA_LANG + "Long";

    /** String. */
    public static final String STRING = PKG_JAVA_LANG + "String";

    /** Double. */
    public static final String DOUBLE = PKG_JAVA_LANG + "Double";

    /**
     * Get type name by package name and class name.
     * @param packageName Package name.
     * @param className Class name.
     * @return Type name.
     */
    public static String typeName(String packageName, String className) {
        return String.format("%s.%s", packageName, className);
    }

}
