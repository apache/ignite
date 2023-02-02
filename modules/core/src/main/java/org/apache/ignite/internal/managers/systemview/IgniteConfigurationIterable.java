package org.apache.ignite.internal.managers.systemview;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Responsibility of this class it to recursively iterate {@link IgniteConfiguration} object
 * and expose all properties in for of String pairs.
 */
public class IgniteConfigurationIterable implements Iterable<String[]> {
    /** Configuration. */
    private final IgniteConfiguration cfg;

    /** */
    private final Queue<GridTuple3<Object, Iterator<Field>, String>> iters = new LinkedList<>();

    /** */
    private final Set<Object> visited = new HashSet<>();

    /**
     * @param cfg Configuration to iterate.
     */
    public IgniteConfigurationIterable(IgniteConfiguration cfg) {
        this.cfg = cfg;

        tryAddToQueue(null, cfg, "");
    }

    /** {@inheritDoc} */
    @Override public Iterator<String[]> iterator() {
        return new Iterator<String[]>() {
            private String[] next;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                advance();

                return next != null;
            }

            private void advance() {
                if (next != null)
                    return;

                while (!iters.isEmpty() && !iters.peek().get2().hasNext())
                    iters.remove();

                if (!iters.isEmpty()) {
                    GridTuple3<Object, Iterator<Field>, String> curr = iters.peek();

                    Field f = curr.get2().next();

                    try {
                        f.setAccessible(true);

                        Object val = f.get(curr.get1());

                        if (val instanceof IgniteLogger
                            || val instanceof ClusterNode
                            || val instanceof IgniteKernal
                            || val instanceof GridKernalContext
                            || val instanceof MetricRegistry)
                            advance();
                        else {
                            boolean res = tryAddToQueue(
                                f,
                                val,
                                (curr.get3().isEmpty() ? f.getName() : metricName(curr.get3(), f.getName()))
                            );

                            if (res)
                                advance();
                            else
                                next = new String[]{
                                    curr.get3().isEmpty() ? f.getName() : (curr.get3() + "." + f.getName()),
                                    U.toStringSafe(val)
                                };
                        }

                    }
                    catch (IllegalAccessException e) {
                        throw new IgniteException(e);
                    }
                }
            }

            /** {@inheritDoc} */
            @Override public String[] next() {
                if (next == null)
                    advance();

                String[] next0 = next;

                if (next0 == null)
                    throw new NoSuchElementException();

                next = null;

                return next0;
            }
        };
    }

    /** */
    private boolean tryAddToQueue(Field f, Object val, String prefix) {
        if (val == null)
            return false;

        Class<?> clz = f == null ? val.getClass() : f.getType();

        if (clz.isArray() || !clz.getName().startsWith("org.apache.ignite"))
            return false;

        if (visited.add(val))
            iters.add(F.t(val, allFieldsIterator(val), prefix));

        return true;
    }

    /**
     * @param obj Object to iterate fields.
     * @return Iterator of object fields.
     */
    private Iterator<Field> allFieldsIterator(Object obj) {
        if (obj == null)
            return Collections.emptyIterator();

        return F.iterator0(
            Arrays.asList(obj.getClass().getDeclaredFields()),
            true,
            f -> !Modifier.isStatic(f.getModifiers())
        );
    }
}
