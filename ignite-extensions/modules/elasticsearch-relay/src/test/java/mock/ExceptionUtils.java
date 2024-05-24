package mock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

public class ExceptionUtils {
	
	/**  
     * 尝试执行给定的Runnable，并期望它抛出特定类型的异常。  
     * 如果Runnable执行过程中没有抛出异常，或者抛出了非预期类型的异常，  
     * 则断言失败。  
     *  
     * @param expectedExceptionClass 预期的异常类型  
     * @param runnable               要执行的Runnable  
     * @param <E>                    预期的异常类型  
     * @throws E 如果Runnable执行过程中抛出了预期类型的异常，则抛出此异常  
     * @throws AssertionError 如果Runnable没有抛出异常或者抛出了非预期类型的异常  
     */  
    public static <E extends Throwable> E expectThrows(Class<E> expectedExceptionClass, Runnable runnable)  {  
        try {  
            runnable.run();  
        } catch (Throwable thrown) {  
            // 检查是否抛出了预期的异常类型  
            if (expectedExceptionClass.isInstance(thrown)) {  
                // 如果抛出了预期类型的异常，则直接抛出它  
                return (E) thrown;  
            } else {  
                // 如果抛出了非预期类型的异常，则断言失败  
                fail("Expected exception of type " + expectedExceptionClass.getName()  
                        + " but caught " + thrown.getClass().getName());  
            }  
        }  
        // 如果没有抛出任何异常，则断言失败  
        fail("Expected exception of type " + expectedExceptionClass.getName() + " but none was thrown"); 
        return null;
    }  
  
    /**  
     * 尝试执行给定的Callable，并期望它抛出特定类型的异常。  
     * 如果Callable执行过程中没有抛出异常，或者抛出了非预期类型的异常，  
     * 则断言失败。  
     *  
     * @param expectedExceptionClass 预期的异常类型  
     * @param callable               要执行的Callable  
     * @param <T>                    Callable的返回类型  
     * @param <E>                    预期的异常类型  
     * @return 如果Callable执行过程中没有抛出异常，则返回null（但通常不会达到这里）  
     * @throws E 如果Callable执行过程中抛出了预期类型的异常，则抛出此异常  
     * @throws AssertionError 如果Callable没有抛出异常或者抛出了非预期类型的异常  
     */  
    public static <T, E extends Throwable> E expectThrows(Class<E> expectedExceptionClass, Callable<T> callable) {  
        try {  
            callable.call();  
        } catch (Throwable thrown) {  
            // 检查是否抛出了预期的异常类型  
            if (expectedExceptionClass.isInstance(thrown)) {  
                // 如果抛出了预期类型的异常，则直接抛出它  
                return (E)thrown;  
            } else {  
                // 如果抛出了非预期类型的异常，则断言失败  
                fail("Expected exception of type " + expectedExceptionClass.getName()  
                        + " but caught " + thrown.getClass().getName());  
            }  
        }  
        // 如果没有抛出任何异常，则断言失败  
        fail("Expected exception of type " + expectedExceptionClass.getName() + " but none was thrown"); 
        return null;
    }  
  
    // 一个简单的断言失败方法，用于模拟JUnit的断言失败行为  
    private static void fail(String message) {  
        throw new AssertionError(message);  
    } 
    
    

    /**
     * Runs the code block for 10 seconds waiting for no assertion to trip.
     */
    public static void assertBusy(CheckedRunnable<Exception> codeBlock) throws Exception {
        assertBusy(codeBlock, 10, TimeUnit.SECONDS);
    }

    /**
     * Runs the code block for the provided interval, waiting for no assertions to trip.
     */
    public static void assertBusy(CheckedRunnable<Exception> codeBlock, long maxWaitTime, TimeUnit unit) throws Exception {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        // In case you've forgotten your high-school studies, log10(x) / log10(y) == log y(x)
        long iterations = Math.max(Math.round(Math.log10(maxTimeInMillis) / Math.log10(2)), 1);
        long timeInMillis = 1;
        long sum = 0;
        List<AssertionError> failures = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            try {
                codeBlock.run();
                return;
            } catch (AssertionError e) {
                failures.add(e);
            }
            sum += timeInMillis;
            Thread.sleep(timeInMillis);
            timeInMillis *= 2;
        }
        timeInMillis = maxTimeInMillis - sum;
        Thread.sleep(Math.max(timeInMillis, 0));
        try {
            codeBlock.run();
        } catch (AssertionError e) {
            for (AssertionError failure : failures) {
                e.addSuppressed(failure);
            }
            throw e;
        }
    }

    /**
     * Runs the code block for the provided max wait time and sleeping for fixed sleep time, waiting for no assertions to trip.
     */
    public static void assertBusyWithFixedSleepTime(CheckedRunnable<Exception> codeBlock, TimeValue maxWaitTime, TimeValue sleepTime)
        throws Exception {
        long maxTimeInMillis = maxWaitTime.millis();
        long sleepTimeInMillis = sleepTime.millis();
        if (sleepTimeInMillis > maxTimeInMillis) {
            throw new IllegalArgumentException("sleepTime is more than the maxWaitTime");
        }
        long sum = 0;
        List<AssertionError> failures = new ArrayList<>();
        while (sum <= maxTimeInMillis) {
            try {
                codeBlock.run();
                return;
            } catch (AssertionError e) {
                failures.add(e);
            }
            sum += sleepTimeInMillis;
            Thread.sleep(sleepTimeInMillis);
        }
        try {
            codeBlock.run();
        } catch (AssertionError e) {
            for (AssertionError failure : failures) {
                e.addSuppressed(failure);
            }
            throw e;
        }
    }

    /**
     * Periodically execute the supplied function until it returns true, or a timeout
     * is reached. This version uses a timeout of 10 seconds. If at all possible,
     * use {@link OpenSearchTestCase#assertBusy(CheckedRunnable)} instead.
     *
     * @param breakSupplier determines whether to return immediately or continue waiting.
     * @return the last value returned by <code>breakSupplier</code>
     * @throws InterruptedException if any sleep calls were interrupted.
     */
    public static boolean waitUntil(BooleanSupplier breakSupplier) throws InterruptedException {
        return waitUntil(breakSupplier, 10, TimeUnit.SECONDS);
    }

    // After 1s, we stop growing the sleep interval exponentially and just sleep 1s until maxWaitTime
    private static final long AWAIT_BUSY_THRESHOLD = 1000L;

    /**
     * Periodically execute the supplied function until it returns true, or until the
     * specified maximum wait time has elapsed. If at all possible, use
     * {@link OpenSearchTestCase#assertBusy(CheckedRunnable)} instead.
     *
     * @param breakSupplier determines whether to return immediately or continue waiting.
     * @param maxWaitTime the maximum amount of time to wait
     * @param unit the unit of tie for <code>maxWaitTime</code>
     * @return the last value returned by <code>breakSupplier</code>
     * @throws InterruptedException if any sleep calls were interrupted.
     */
    public static boolean waitUntil(BooleanSupplier breakSupplier, long maxWaitTime, TimeUnit unit) throws InterruptedException {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        long timeInMillis = 1;
        long sum = 0;
        while (sum + timeInMillis < maxTimeInMillis) {
            if (breakSupplier.getAsBoolean()) {
                return true;
            }
            Thread.sleep(timeInMillis);
            sum += timeInMillis;
            timeInMillis = Math.min(AWAIT_BUSY_THRESHOLD, timeInMillis * 2);
        }
        timeInMillis = maxTimeInMillis - sum;
        Thread.sleep(Math.max(timeInMillis, 0));
        return breakSupplier.getAsBoolean();
    }

    public static boolean terminate(ExecutorService... services) {
        boolean terminated = true;
        for (ExecutorService service : services) {
            if (service != null) {
                terminated &= ThreadPool.terminate(service, 10, TimeUnit.SECONDS);
            }
        }
        return terminated;
    }

    public static boolean terminate(ThreadPool threadPool) {
        return ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
    
    public static boolean randomBoolean() {
		Random r = new Random();
		return r.nextBoolean();
	}
    
    public static String toString(InputStream stream) {
        StringBuilder textBuilder = new StringBuilder();
        try (Reader reader = new BufferedReader(new InputStreamReader(stream, Charset.forName(StandardCharsets.UTF_8.name())))) {
            int c;
            while ((c = reader.read()) != -1) {
                textBuilder.append((char) c);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return textBuilder.toString();
    }

}
