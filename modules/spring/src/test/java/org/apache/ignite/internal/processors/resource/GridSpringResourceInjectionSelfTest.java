package org.apache.ignite.internal.processors.resource;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Tests for injected resource.
 */
public class GridSpringResourceInjectionSelfTest extends GridCommonAbstractTest {
    /** Bean name. */
    private static final String DUMMY_BEAN = "dummyResourceBean";

    /** Test grid with Spring context. */
    private static Ignite grid;

    /** {@inheritDoc} */
    @Override public void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        grid = IgniteSpring.start(new ClassPathXmlApplicationContext("/org/apache/ignite/internal/processors/resource/spring-resource.xml"));
    }

    /** {@inheritDoc} */
    @Override public void afterTestsStopped() throws Exception {
        G.stop(grid.name(), true);

        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureField() throws Exception {
        grid.compute().call(new IgniteCallable<Object>() {
            @SpringResource(resourceName = DUMMY_BEAN)
            private transient DummyResourceBean dummyResourceBean;

            @Override public Object call() throws Exception {
                assertNotNull(dummyResourceBean);

                return null;
            }
        });
    }

    /**
     * Resource injection with non-existing resource name.
     */
    public void testClosureFieldWithWrongResourceName() {
        Exception ex = null;
        try {
            grid.compute().call(new IgniteCallable<Object>() {
                @SpringResource(resourceName = "")
                private transient DummyResourceBean dummyResourceBean;

                @Override public Object call() throws Exception {
                    return null;
                }
            });
        }
        catch (IgniteException expected) {
            ex = expected;
        }
        assertNotNull(ex);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethod() throws Exception {
        grid.compute().call(new IgniteCallable<Object>() {
            private DummyResourceBean dummyResourceBean;

            @SpringResource(resourceName = DUMMY_BEAN)
            private void setDummyResourceBean(DummyResourceBean dummyResourceBean) {
                assertNotNull(dummyResourceBean);

                this.dummyResourceBean = dummyResourceBean;
            }

            @Override public Object call() throws Exception {
                return null;
            }
        });
    }

    /**
     * Resource injection with non-existing resource name.
     */
    public void testClosureMethodWithWrongResourceName() {
        Exception ex = null;
        try {
            grid.compute().call(new IgniteCallable<Object>() {
                private DummyResourceBean dummyResourceBean;

                @SpringResource(resourceName = "")
                private void setDummyResourceBean(DummyResourceBean dummyResourceBean) {
                }

                @Override public Object call() throws Exception {
                    return null;
                }
            });
        }
        catch (IgniteException expected) {
            ex = expected;
        }
        assertNotNull(ex);
    }

    /**
     * Dummy resource bean.
     */
    public static class DummyResourceBean {
        public DummyResourceBean() {
        }
    }
}
