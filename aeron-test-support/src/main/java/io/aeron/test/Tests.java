/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.test;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.exceptions.TimeoutException;
import org.agrona.LangUtil;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import javax.management.Attribute;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import static org.mockito.Mockito.doAnswer;

/**
 * Utilities to help with writing tests.
 */
public class Tests
{
    public static final IdleStrategy SLEEP_1_MS = new SleepingMillisIdleStrategy(1);
    private static final String LOGGING_MBEAN_NAME = "io.aeron:type=logging";

    /**
     * Set a private field in a class for testing.
     *
     * @param instance  of the object to set the field value.
     * @param fieldName to be set.
     * @param value     to be set on the field.
     */
    public static void setField(final Object instance, final String fieldName, final Object value)
    {
        try
        {
            final Field field = instance.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(instance, value);
        }
        catch (final Throwable t)
        {
            LangUtil.rethrowUnchecked(t);
        }
    }

    /**
     * Error handler that can be used as an implementation of {@link org.agrona.ErrorHandler} which will print out
     * a stacktrace unless the exception is to type {@link AeronException.Category#WARN}.
     *
     * @param ex to be handled.
     */
    public static void onError(final Throwable ex)
    {
        if (ex instanceof AeronException && ((AeronException)ex).category() == AeronException.Category.WARN)
        {
            //System.out.println("Warning: " + ex.getMessage());
            return;
        }

        ex.printStackTrace();
    }

    /**
     * Check if the interrupt flag has been set on the current thread and fail the test if it has.
     * <p>
     * This is useful for terminating tests stuck in a loop on timeout otherwise JUnit will proceed to the next test
     * and leave the thread spinning and consuming CPU resource.
     */
    public static void checkInterruptStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new TimeoutException("unexpected interrupt");
        }
    }

    /**
     * Check if the interrupt flag has been set on the current thread and fail the test if it has.
     * <p>
     * This is useful for terminating tests stuck in a loop on timeout otherwise JUnit will proceed to the next test
     * and leave the thread spinning and consuming CPU resource.
     *
     * @param messageSupplier additional context information to include in the failure message
     */
    public static void checkInterruptStatus(final Supplier<String> messageSupplier)
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new TimeoutException(messageSupplier.get());
        }
    }

    /**
     * Check if the interrupt flag has been set on the current thread and fail the test if it has.
     * <p>
     * This is useful for terminating tests stuck in a loop on timeout otherwise JUnit will proceed to the next test
     * and leave the thread spinning and consuming CPU resource.
     *
     * @param format A format string, {@link java.util.Formatter} to use as additional context information in the
     *               failure message
     * @param args   arguments to the format string
     */
    public static void checkInterruptStatus(final String format, final Object... args)
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new TimeoutException(String.format(format, args));
        }
    }

    /**
     * Check if the interrupt flag has been set on the current thread and fail the test if it has.
     * <p>
     * This is useful for terminating tests stuck in a loop on timeout otherwise JUnit will proceed to the next test
     * and leave the thread spinning and consuming CPU resource.
     *
     * @param message to provide additional context on unexpected interrupt.
     */
    public static void checkInterruptStatus(final String message)
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new TimeoutException(message);
        }
    }

    /**
     * Print out the message and stack trace on thread interrupt.
     *
     * @param message to provide additional context on unexpected interrupt.
     */
    public static void unexpectedInterruptStackTrace(final String message)
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("*** unexpected interrupt");

        if (null != message)
        {
            sb.append(" - ").append(message);
        }

        appendStackTrace(sb).append('\n');
    }

    /**
     * Append the current thread stack trace to a {@link StringBuilder}.
     *
     * @param sb to append the stack trace to.
     * @return the builder for a fluent API.
     */
    public static StringBuilder appendStackTrace(final StringBuilder sb)
    {
        return appendStackTrace(sb, Thread.currentThread().getStackTrace());
    }

    /**
     * Append a thread stack trace to a {@link StringBuilder}.
     *
     * @param sb                 to append the stack trace to.
     * @param stackTraceElements to be appended.
     * @return the builder for a fluent API.
     */
    public static StringBuilder appendStackTrace(final StringBuilder sb, final StackTraceElement[] stackTraceElements)
    {
        sb.append(System.lineSeparator());

        for (int i = 1, length = stackTraceElements.length; i < length; i++)
        {
            sb.append(stackTraceElements[i]).append(System.lineSeparator());
        }

        return sb;
    }

    /**
     * Same as {@link Thread#sleep(long)} but without the checked exception.
     *
     * @param durationMs to sleep.
     */
    public static void sleep(final long durationMs)
    {
        try
        {
            Thread.sleep(durationMs);
        }
        catch (final InterruptedException ex)
        {
            throw new TimeoutException(ex, AeronException.Category.ERROR);
        }
    }

    /**
     * Same as {@link Thread#sleep(long)} but without the checked exception.
     *
     * @param durationMs      to sleep.
     * @param messageSupplier of message to be reported on interrupt.
     */
    public static void sleep(final long durationMs, final Supplier<String> messageSupplier)
    {
        try
        {
            Thread.sleep(durationMs);
        }
        catch (final InterruptedException ex)
        {
            throw new TimeoutException(messageSupplier.get());
        }
    }

    /**
     * Same as {@link Thread#sleep(long)} but without the checked exception.
     *
     * @param durationMs to sleep.
     * @param format     of the message.
     * @param params     to be formatted.
     */
    public static void sleep(final long durationMs, final String format, final Object... params)
    {
        try
        {
            Thread.sleep(durationMs);
        }
        catch (final InterruptedException ex)
        {
            throw new TimeoutException(String.format(format, params));
        }
    }

    /**
     * Yield the thread then check for interrupt in a test.
     *
     * @see #checkInterruptStatus()
     */
    public static void yield()
    {
        Thread.yield();
        checkInterruptStatus();
    }

    /**
     * Helper method to mock {@link AutoCloseable#close()} method to throw exception.
     *
     * @param mock      to have it's method mocked
     * @param exception exception to be thrown
     * @throws Exception to make compiler happy
     */
    public static void throwOnClose(final AutoCloseable mock, final Throwable exception) throws Exception
    {
        doAnswer(
            (invocation) ->
            {
                LangUtil.rethrowUnchecked(exception);
                return null;
            }).when(mock).close();
    }

    /**
     * {@link IdleStrategy#idle()} the provide strategy and check for thread interrupt after.
     *
     * @param idleStrategy    to be used for the idle operation.
     * @param messageSupplier to be used in the event of interrupt.
     */
    public static void idle(final IdleStrategy idleStrategy, final Supplier<String> messageSupplier)
    {
        idleStrategy.idle();
        checkInterruptStatus(messageSupplier);
    }

    /**
     * {@link IdleStrategy#idle()} the provide strategy and check for thread interrupt after.
     *
     * @param idleStrategy to be used for the idle operation.
     * @param format       of the message to be used in the event of interrupt.
     * @param params       to be substituted into the message format.
     */
    public static void idle(final IdleStrategy idleStrategy, final String format, final Object... params)
    {
        idleStrategy.idle();
        checkInterruptStatus(format, params);
    }

    /**
     * {@link IdleStrategy#idle()} the provide strategy and check for thread interrupt after.
     *
     * @param idleStrategy to be used for the idle operation.
     * @param message      to be used in the event of interrupt.
     */
    public static void idle(final IdleStrategy idleStrategy, final String message)
    {
        idleStrategy.idle();
        checkInterruptStatus(message);
    }

    /**
     * Call {@link YieldingIdleStrategy#idle()} and check for thread interrupt after.
     *
     * @param messageSupplier to be used in the event of interrupt.
     */
    public static void yieldingIdle(final Supplier<String> messageSupplier)
    {
        idle(YieldingIdleStrategy.INSTANCE, messageSupplier);
    }

    /**
     * Call {@link YieldingIdleStrategy#idle()} and check for thread interrupt after.
     *
     * @param format of the message to be used in the event of interrupt.
     * @param params to be substituted into the message format.
     */
    public static void yieldingIdle(final String format, final Object... params)
    {
        idle(YieldingIdleStrategy.INSTANCE, format, params);
    }

    /**
     * Call {@link YieldingIdleStrategy#idle()} and check for thread interrupt after.
     *
     * @param message to be used in the event of interrupt.
     */
    public static void yieldingIdle(final String message)
    {
        idle(YieldingIdleStrategy.INSTANCE, message);
    }

    /**
     * Execute a task until a condition is satisfied, or a maximum number of iterations, or a timeout is reached.
     *
     * @param condition         keep executing while true.
     * @param iterationConsumer to be invoked with the iteration count.
     * @param maxIterations     to be executed.
     * @param timeoutNs         to stay within.
     */
    public static void executeUntil(
        final BooleanSupplier condition,
        final IntConsumer iterationConsumer,
        final int maxIterations,
        final long timeoutNs)
    {
        final long startNs = System.nanoTime();
        long nowNs;
        int i = 0;

        do
        {
            checkInterruptStatus();
            iterationConsumer.accept(i);
            nowNs = System.nanoTime();
        }
        while (!condition.getAsBoolean() && ((nowNs - startNs) < timeoutNs) && i++ < maxIterations);
    }

    /**
     * Await a condition with a timeout and also check for thread interrupt.
     *
     * @param conditionSupplier for the condition to be awaited.
     * @param timeoutNs         to await.
     */
    public static void await(final BooleanSupplier conditionSupplier, final long timeoutNs)
    {
        final long deadlineNs = System.nanoTime() + timeoutNs;
        while (!conditionSupplier.getAsBoolean())
        {
            if ((deadlineNs - System.nanoTime()) <= 0)
            {
                throw new TimeoutException();
            }

            Tests.yield();
        }
    }

    /**
     * Await a condition with a check for thread interrupt.
     *
     * @param conditionSupplier for the condition to be awaited.
     */
    public static void await(final BooleanSupplier conditionSupplier)
    {
        while (!conditionSupplier.getAsBoolean())
        {
            Tests.yield();
        }
    }

    /**
     * Await a counter reaching or passing a value while checking for thread interrupt.
     *
     * @param counter to be evaluated.
     * @param value   as threshold to awaited.
     */
    public static void awaitValue(final AtomicLong counter, final long value)
    {
        long counterValue;
        while ((counterValue = counter.get()) < value)
        {
            Thread.yield();
            if (Thread.currentThread().isInterrupted())
            {
                throw new TimeoutException("awaiting=" + value + " counter=" + counterValue);
            }
        }
    }

    /**
     * Await a counter reaching or passing a value while checking for thread interrupt.
     *
     * @param counter to be evaluated.
     * @param value   as threshold to awaited.
     */
    public static void awaitValue(final AtomicCounter counter, final long value)
    {
        long counterValue;
        while ((counterValue = counter.get()) < value)
        {
            Thread.yield();
            if (Thread.currentThread().isInterrupted())
            {
                throw new TimeoutException("awaiting=" + value + " counter=" + counterValue);
            }

            if (counter.isClosed())
            {
                unexpectedInterruptStackTrace("awaiting=" + value + " counter=" + counterValue);
            }
        }
    }

    /**
     * Await a counter increasing by a delta that will sleep and check for thread interrupt.
     *
     * @param counters  reader over all counters.
     * @param counterId of the specific counter to be read.
     * @param delta     increase to await from initial value.
     */
    public static void awaitCounterDelta(final CountersReader counters, final int counterId, final long delta)
    {
        awaitCounterDelta(counters, counterId, counters.getCounterValue(counterId), delta);
    }

    /**
     * Await a counter increasing by a delta that will sleep and check for thread interrupt.
     *
     * @param counters     reader over all counters.
     * @param counterId    of the specific counter to be read.
     * @param initialValue from which the delta will be tracked.
     * @param delta        increase to await from initial value.
     */
    public static void awaitCounterDelta(
        final CountersReader counters, final int counterId, final long initialValue, final long delta)
    {
        final long expectedValue = initialValue + delta;
        final Supplier<String> counterMessage = () ->
            "timed out waiting for '" + counters.getCounterLabel(counterId) + "' to reach " + expectedValue;

        while (counters.getCounterValue(counterId) < expectedValue)
        {
            idle(SLEEP_1_MS, counterMessage);
        }
    }

    /**
     * Repeat the attempt to re-add a subscription until successful if it fails with a warning
     * {@link RegistrationException} which could be due to a port clash.
     *
     * @param aeron    to add the subscription on.
     * @param channel  for the subscription.
     * @param streamId for the subscription.
     * @return the added subscription.
     */
    public static Subscription reAddSubscription(final Aeron aeron, final String channel, final int streamId)
    {
        while (true)
        {
            try
            {
                return aeron.addSubscription(channel, streamId);
            }
            catch (final RegistrationException ex)
            {
                if (ex.category() != AeronException.Category.WARN)
                {
                    throw ex;
                }

                yieldingIdle(ex.getMessage());
            }
        }
    }

    /**
     * Await a Publication being connected by yielding and checking for thread interrupt.
     *
     * @param publication to await being connected.
     */
    public static void awaitConnected(final Publication publication)
    {
        while (!publication.isConnected())
        {
            Tests.yield();
        }
    }

    /**
     * Await a Subscription being connected by yielding and checking for thread interrupt.
     *
     * @param subscription to await being connected.
     */
    public static void awaitConnected(final Subscription subscription)
    {
        while (!subscription.isConnected())
        {
            Tests.yield();
        }
    }

    /**
     * Await a Subscription have a minimum number of connections by yielding and checking for thread interrupt.
     *
     * @param subscription    to await being connected.
     * @param connectionCount to await.
     */
    public static void awaitConnections(final Subscription subscription, final int connectionCount)
    {
        while (subscription.imageCount() < connectionCount)
        {
            Tests.yield();
        }
    }

    /**
     * Generates a string value that is a prefix with a suffix appended a number of times.
     *
     * @param prefix            for the beginning of the string.
     * @param suffix            for the end of the string.
     * @param suffixRepeatCount for number of times the suffix is appended.
     * @return the generated string.
     */
    public static String generateStringWithSuffix(final String prefix, final String suffix, final int suffixRepeatCount)
    {
        final StringBuilder builder = new StringBuilder(prefix.length() + (suffix.length() * suffixRepeatCount));

        builder.append(prefix);

        for (int i = 0; i < suffixRepeatCount; i++)
        {
            builder.append(suffix);
        }

        return builder.toString();
    }

    /**
     * Start the collecting log of debug events for a test run.
     */
    public static void startLogCollecting()
    {
        try
        {
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            final ObjectName loggingName = new ObjectName(LOGGING_MBEAN_NAME);

            try
            {
                mBeanServer.setAttribute(loggingName, new Attribute("Collecting", true));
            }
            catch (final InstanceNotFoundException ignore)
            {
                // It must not have been set up for the test. Expected in many cases.
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Reset the collecting of logs for a new test run.
     */
    public static void resetLogCollecting()
    {
        try
        {
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            final ObjectName loggingName = new ObjectName(LOGGING_MBEAN_NAME);

            try
            {
                mBeanServer.invoke(loggingName, "reset", new Object[0], new String[0]);
            }
            catch (final InstanceNotFoundException ignore)
            {
                // It must not have been set up for the test. Expected in many cases.
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Dump the collected log of events to a file.
     *
     * @param filename to dump the log of events to.
     */
    public static void dumpCollectedLogs(final String filename)
    {
        try
        {
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            final ObjectName loggingName = new ObjectName(LOGGING_MBEAN_NAME);

            try
            {
                mBeanServer.invoke(
                    loggingName, "writeToFile", new Object[]{ filename }, new String[]{ "java.lang.String" });
            }
            catch (final InstanceNotFoundException ignore)
            {
                // It must not have been set up for the test. Expected in many cases.
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
