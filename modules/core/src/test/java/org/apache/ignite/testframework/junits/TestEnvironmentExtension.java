package org.apache.ignite.testframework.junits;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.lang.reflect.Method;

/** */
public class TestEnvironmentExtension implements BeforeEachCallback, AfterEachCallback, InvocationInterceptor {
    @Override
    public void beforeEach(ExtensionContext context) {
        prepareTestEnvironment();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        cleanUpTestEnvironment();
    }

    @Override
    public void interceptTestMethod(
            Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext,
            ExtensionContext extensionContext
    ) throws Throwable {
        invocation.proceed();
    }

    private void prepareTestEnvironment() {
        // Your test environment preparation logic
    }

    private void cleanUpTestEnvironment() {
        // Your test environment cleanup logic
    }
}
