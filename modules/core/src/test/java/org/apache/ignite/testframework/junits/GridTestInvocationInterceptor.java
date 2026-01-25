package org.apache.ignite.testframework.junits;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public class GridTestInvocationInterceptor implements BeforeAllCallback, InvocationInterceptor {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(GridTestInvocationInterceptor.class);

    private static final String FIXTURE_KEY = "fixture";

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Store fixture instance in context for use in interceptTestTemplateMethod
        Constructor<?> testConstructor = context.getRequiredTestClass().getDeclaredConstructor();
        testConstructor.setAccessible(true);
        GridAbstractTest fixtureInstance = (GridAbstractTest) testConstructor.newInstance();

        context.getStore(NAMESPACE).put(FIXTURE_KEY, fixtureInstance);
    }

    @Override
    public void interceptTestTemplateMethod(
            Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext,
            ExtensionContext extensionContext
    ) throws Throwable {
        GridAbstractTest fixtureInstance = extensionContext.getStore(NAMESPACE)
                .get(FIXTURE_KEY, GridAbstractTest.class);

        Throwable suppressed = null;

        try {
            fixtureInstance.beforeFirstTest();
            invocation.proceed();
        }
        catch (Throwable t) {
            suppressed = t;
            throw t;
        }
        finally {
            try {
                fixtureInstance.afterLastTest();
            }
            catch (Throwable t) {
                if (suppressed != null) {
                    t.addSuppressed(suppressed);
                }
                throw t;
            }
        }
    }
}
