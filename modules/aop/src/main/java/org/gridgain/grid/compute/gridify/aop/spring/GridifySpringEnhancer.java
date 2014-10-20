/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute.gridify.aop.spring;

import org.gridgain.grid.compute.gridify.*;
import org.gridgain.grid.compute.gridify.aop.spring.GridifySpringPointcut.*;
import org.springframework.aop.framework.*;
import org.springframework.aop.support.*;

/**
 * Spring AOP enhancer. Use it to grid-enable methods annotated with
 * {@link Gridify}, {@link GridifySetToValue} and {@link GridifySetToSet} annotations.
 * <p>
 * Note, that Spring AOP requires that all grid-enabled methods must
 * be {@code enhanced} because it is proxy-based. Other AOP implementations,
 * such as JBoss or AspectJ don't require special handling.
 * <p>
 * See {@link Gridify} documentation for more information about execution of
 * {@code gridified} methods.
 * @see Gridify
 * @see GridifySetToValue
 * @see GridifySetToSet
 */
public final class GridifySpringEnhancer {
    /** Spring aspect. */
    private static final GridifySpringAspect dfltAsp = new GridifySpringAspect();

    /** Spring aspect. */
    private static final GridifySetToSetSpringAspect setToSetAsp = new GridifySetToSetSpringAspect();

    /** Spring aspect. */
    private static final GridifySetToValueSpringAspect setToValAsp = new GridifySetToValueSpringAspect();

    /**
     * Enforces singleton.
     */
    private GridifySpringEnhancer() {
        // No-op.
    }

    /**
     * Enhances the object on load.
     *
     * @param <T> Type of the object to enhance.
     * @param obj Object to augment/enhance.
     * @return Enhanced object.
     */
    @SuppressWarnings({"unchecked"})
    public static <T> T enhance(T obj) {
        ProxyFactory proxyFac = new ProxyFactory(obj);

        proxyFac.addAdvice(dfltAsp);
        proxyFac.addAdvice(setToValAsp);
        proxyFac.addAdvice(setToSetAsp);

        while (proxyFac.getAdvisors().length > 0)
            proxyFac.removeAdvisor(0);

        proxyFac.addAdvisor(new DefaultPointcutAdvisor(
            new GridifySpringPointcut(GridifySpringPointcutType.DFLT), dfltAsp));
        proxyFac.addAdvisor(new DefaultPointcutAdvisor(
            new GridifySpringPointcut(GridifySpringPointcutType.SET_TO_VALUE), setToValAsp));
        proxyFac.addAdvisor(new DefaultPointcutAdvisor(
            new GridifySpringPointcut(GridifySpringPointcutType.SET_TO_SET), setToSetAsp));

        return (T)proxyFac.getProxy();
    }
}
