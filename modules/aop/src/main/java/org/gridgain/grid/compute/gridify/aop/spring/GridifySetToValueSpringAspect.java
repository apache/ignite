/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute.gridify.aop.spring;

import org.aopalliance.intercept.*;
import org.apache.ignite.*;
import org.gridgain.grid.compute.gridify.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.gridify.aop.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.gridify.*;
import java.lang.reflect.*;

import static org.apache.ignite.IgniteState.*;
import static org.gridgain.grid.util.gridify.GridifyUtils.*;

/**
 * Spring aspect that cross-cuts on all methods grid-enabled with
 * {@link GridifySetToValue} annotation and potentially executes them on
 * remote node.
 * <p>
 * Note that Spring uses proxy-based AOP, so in order to be properly
 * cross-cut, all methods need to be enhanced with {@link GridifySpringEnhancer}
 * helper.
 * <p>
 * See {@link GridifySetToValue} documentation for more information about execution of
 * {@code gridified} methods.
 * @see GridifySetToValue
 */
public class GridifySetToValueSpringAspect extends GridifySetToValueAbstractAspect implements MethodInterceptor {
    /**
     * Aspect implementation which executes grid-enabled methods on remote
     * nodes.
     *
     * {@inheritDoc}
     */
    @SuppressWarnings({"ProhibitedExceptionDeclared", "ProhibitedExceptionThrown", "CatchGenericClass"})
    @Override public Object invoke(MethodInvocation invoc) throws Throwable {
        Method mtd = invoc.getMethod();

        GridifySetToValue ann = mtd.getAnnotation(GridifySetToValue.class);

        assert ann != null : "Intercepted method does not have gridify annotation.";

        // Since annotations in Java don't allow 'null' as default value
        // we have accept an empty string and convert it here.
        // NOTE: there's unintended behavior when user specifies an empty
        // string as intended grid name.
        // NOTE: the 'ann.gridName() == null' check is added to mitigate
        // annotation bugs in some scripting languages (e.g. Groovy).
        String gridName = F.isEmpty(ann.gridName()) ? null : ann.gridName();

        if (G.state(gridName) != STARTED)
            throw new GridException("Grid is not locally started: " + gridName);

        GridifyNodeFilter nodeFilter = null;

        if (!ann.nodeFilter().equals(GridifyNodeFilter.class))
            nodeFilter = ann.nodeFilter().newInstance();

        GridifyArgumentBuilder argBuilder = new GridifyArgumentBuilder();

        // Creates task argument.
        GridifyRangeArgument arg = argBuilder.createTaskArgument(
            mtd.getDeclaringClass(),
            mtd.getName(),
            mtd.getReturnType(),
            mtd.getParameterTypes(),
            mtd.getParameterAnnotations(),
            invoc.getArguments(),
            invoc.getThis());

        if (!ann.interceptor().equals(GridifyInterceptor.class)) {
            // Check interceptor first.
            if (!ann.interceptor().newInstance().isGridify(ann, arg))
                return invoc.proceed();
        }

        // Proceed locally for negative threshold parameter.
        if (ann.threshold() < 0)
            return invoc.proceed();

        // Analyse where to execute method (remotely or locally).
        if (arg.getInputSize() != UNKNOWN_SIZE && arg.getInputSize() <= ann.threshold())
            return invoc.proceed();

        // Check is split to jobs allowed for input method argument with declared splitSize.
        checkIsSplitToJobsAllowed(arg, ann);

        try {
            Ignite ignite = G.grid(gridName);

            return execute(mtd, ignite.compute(), invoc.getMethod().getDeclaringClass(), arg, nodeFilter,
                ann.threshold(), ann.splitSize(), ann.timeout());
        }
        catch (Throwable e) {
            for (Class<?> ex : invoc.getMethod().getExceptionTypes()) {
                // Descend all levels down.
                Throwable cause = e.getCause();

                while (cause != null) {
                    if (ex.isAssignableFrom(cause.getClass()))
                        throw cause;

                    cause = cause.getCause();
                }

                if (ex.isAssignableFrom(e.getClass()))
                    throw e;
            }

            throw new GridifyRuntimeException("Undeclared exception thrown: " + e.getMessage(), e);
        }
    }
}
