package com.digitalcipher.spiked.timing;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The wait strategies. Busy-spin and yielding wait seem to have about the same accuracy,
 * though, busy-spin may be a tad more accurate at short (~200 µs) delay times. Sleep
 * wait should really only be used for timer-resolutions of 10 ms or greater.
 */
public interface WaitStrategy {

    /**
     * Wait until the given deadline (in nanoseconds from epoch)
     *
     * @param deadline The deadline (in nanoseconds), until which to wait.
     * @return {@code true} if the thread has been interrupted; {@code false} if the thread
     * hasn't been interrupted
     */
    Future<Boolean> waitUntil(long deadline);

    /**
     * Shuts-down the executor service for the wait thread
     */
    void shutdown();

    /**
     * Abstract wait strategy that manages the executor service for the strategies, and makes
     * a common entry-point for the wait methods.
     */
    abstract class AbstractWaitStrategy implements WaitStrategy {
        final ExecutorService executorService;

        /**
         * Creates the executor service
         */
        AbstractWaitStrategy() {
            executorService = Executors.newSingleThreadExecutor();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown() {
            executorService.shutdown();
        }

        /**
         * Actual implementation of the wait strategy
         * @param deadline The nana-second time (ns from epoch) at which to stop waiting
         * @return {@code true} if the thread has been interrupted; {@code false} if the thread
         * hasn't been interrupted
         */
        protected abstract boolean waitFunction(final long deadline);

        /**
         * {@inheritDoc}
         */
        @Override
        public Future<Boolean> waitUntil(final long deadline) {
            return executorService.submit(() -> waitFunction(deadline));
        }
    }

    /**
     * Yielding wait strategy.
     * <p>
     * Spins in the loop, until the deadline is reached. Releases the flow control
     * by means of Thread.yield() call. This strategy is less precise than {@link BusySpinWait}
     * one, but is more scheduler-friendly.
     */
    class YieldingWait extends AbstractWaitStrategy {

        // disable the constructor
        YieldingWait() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean waitFunction(final long deadline) {
            while (deadline > System.nanoTime()) {
                Thread.yield();
                if (Thread.currentThread().isInterrupted()) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * BusySpin wait strategy.
     * <p>Current implementation has a resolution of approximately 5 µs but can take
     * as long as 25 µs to do one loop.</p>
     * <p>
     * Spins in the loop until the deadline is reached. In a multi-core environment,
     * will occupy an entire core. Is more precise than Sleep wait strategy, but
     * consumes more resources.
     */
    class BusySpinWait extends AbstractWaitStrategy {

        // disable the constructor
        BusySpinWait() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean waitFunction(long deadline) {
            // System.nanoTime() takes about 200 ns to 250 ns
            // Thread.currentThread().isInterrupted() takes about 1 µs to 6 µs
            while (deadline > System.nanoTime()) {
                if (Thread.currentThread().isInterrupted()) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Sleep wait strategy.
     * <p>
     * Will release the flow control, giving other threads a possibility of execution
     * on the same processor. Uses less resources than BusySpin wait, but is less
     * precise.
     */
    class SleepWait extends AbstractWaitStrategy {

        // disable the constructor
        SleepWait() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean waitFunction(final long deadline) {
            long sleepTimeNanos = deadline - System.nanoTime();
            if (sleepTimeNanos > 0) {
                long sleepTimeMillis = sleepTimeNanos / 1000000;
                int sleepTimeNano = (int) (sleepTimeNanos - (sleepTimeMillis * 1000000));
                try {
                    Thread.sleep(sleepTimeMillis, sleepTimeNano);
                } catch (InterruptedException e) {
                    return true;
                }
            }
            return false;
        }
    }
}
