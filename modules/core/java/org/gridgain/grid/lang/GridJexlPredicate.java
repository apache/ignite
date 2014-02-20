// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.apache.commons.jexl2.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;
import java.io.*;
import java.util.*;

/**
 * Defines a predicate based on <a href="http://commons.apache.org/jexl/">Apache JEXL 2.0</a> boolean
 * expression.
 * <p>
 * Predicate like closure is a first-class function
 * that is defined with (or closed over) its free variables that are bound to the closure
 * scope at execution.
 * <p>
 * JEXL predicate binds free variables to JEXL context either with default names or with names
 * supplied by the caller. If bind name is not provided the default name is {@code elemN}, where {@code N} is
 * 1-based index of the variable. The following code snippet (using {@code GridJexlPredicate2} as an example):
 * <pre name="code" class="java">
 * ...
 * // Create new JEXL predicate with default binding names.
 * new GridJexlPredicate2&lt;String, String&gt;("elem1.length &gt; 0 elem2.length == elem1.length");
 * ...
 * </pre>
 * is identical to this one:
 * <pre name="code" class="java">
 * ...
 * // Create new JEXL predicate with custom binding names.
 * new GridJexlPredicate2&lt;String, String&gt;("s1.length &gt; 0 && s1.length == s2.length", "s1", "s2");
 * ...
 * </pre>
 * Typedefs and factory methods from {@link GridFunc} class can be used to shorten the generics code
 * and provide for convenient and terse code when working with predicates (as well as with closures and tuples):
 * <pre name="code" class="java">
 * ...
 * // Similar as above using typedefs and factory methods.
 * F.x2("s1.length &gt; 0 && s1.length == s2.length", "s1", "s2");
 * // Additional context.
 * F.x2("s1.length &gt; n && s1.length == s2.length", "s1", "s2").with("n", 10);
 * ...
 * </pre>
 * <h2 class="header">Thread Safety</h2>
 * This class <b>does not</b> provide synchronization and caller must ensure an outside synchronization
 * if this predicate is to be used from multiple threads.
 *
 * @author @java.author
 * @version @java.version
 * @param <T> Type of the free variable, i.e. the element the closure is called on.
 * @see GridFunc#x1(String, String)
 * @see GridFunc#x1(String)
 */
public class GridJexlPredicate<T> extends GridPredicate<T> {
    /** */
    private String var;

    /** */
    private String exprStr;

    /** JEXL expression object. */
    @SuppressWarnings({"InstanceVariableMayNotBeInitializedByReadObject"})
    private transient Expression expr;

    /** */
    private Map<String, Object> map = new HashMap<>();

    /**
     * Creates JEXL predicate with no expression, variable binding or additional context.
     * Note that such predicate will always evaluate to {@code false}.
     */
    public GridJexlPredicate() {
        /* No-op. */
    }

    /**
     * Creates JEXL predicate with given parameters.
     *
     * @param exprStr JEXL boolean expression. Note that non-boolean return value will evaluate this
     *      predicate to {@code false}.
     * @param var Name of the bound variable in JEXL expression.
     */
    public GridJexlPredicate(String exprStr, String var) {
        A.notNull(exprStr, "exprStr", var, "var");

        this.exprStr = exprStr;
        this.var = var;
    }

    /**
     * Compiles string JEXL expression into internal form.
     */
    private void lazyCompile() {
        if (exprStr != null && expr == null) {
            try {
                expr = new JexlEngine().createExpression(exprStr);
            }
            catch (Exception e) {
                throw new GridRuntimeException("Failed to parse JEXL expression: " + exprStr, e);
            }
        }
    }

    /**
     * Creates JEXL predicate with given parameters. Name of the bound variable will be {@code elem}.
     *
     * @param exprStr JEXL boolean expression. Note that non-boolean return value will evaluate this
     *      predicate to {@code false}.
     */
    public GridJexlPredicate(String exprStr) {
        this(exprStr, "elem");
    }

    /**
     * Sets JEXL context variable value and returns {@code this}.
     *
     * @param var Name of the variable in JEXL context (new or existing).
     * @param val Value to be set or overridden in JEXL context.
     * @return This predicate so that this call can be chained.
     */
    public GridJexlPredicate<T> with(String var, @Nullable Object val) {
        A.notNull(var, "var");

        map.put(var, val);

        return this;
    }

    /**
     * Sets JEXL context variables' values and returns {@code this}.
     *
     * @param vals Set of tuples representing JEXL context to be bound. First element
     *      in the tuple represents the name and the second element represents its value in the context.
     * @return This predicate so that this call can be chained.
     */
    public GridJexlPredicate<T> with(GridBiTuple<String, Object>... vals) {
        for (GridBiTuple<String, Object> t : vals) {
            map.put(t.get1(), t.get2());
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(@Nullable T t) {
        lazyCompile();

        if (expr != null) {
            JexlContext ctx = new MapContext();

            ctx.set(var, t);

            for (Map.Entry<String, Object> e : map.entrySet()) {
                ctx.set(e.getKey(), e.getValue());
            }

            try {
                Object obj = expr.evaluate(ctx);

                if (obj instanceof Boolean) {
                    return (Boolean)obj;
                }
            }
            catch (Exception ex) {
                F.wrap(ex);
            }
        }

        return false;
    }

    /**
     * @param out The stream to write the object to.
     * @throws IOException Includes any I/O exceptions that may occur.
     */
    private void writeObject(ObjectOutput out) throws IOException {
        U.writeString(out, var);
        U.writeMap(out, map);
        U.writeString(out, exprStr);
    }

    /**
     * @param in The stream to read data from in order to restore the object.
     * @throws IOException If I/O errors occur.
     * @throws ClassNotFoundException If the class for an object being restored cannot be found.
     */
    private void readObject(ObjectInput in) throws IOException, ClassNotFoundException {
        var = U.readString(in);
        map = U.readMap(in);
        exprStr = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJexlPredicate.class, this);
    }
}
