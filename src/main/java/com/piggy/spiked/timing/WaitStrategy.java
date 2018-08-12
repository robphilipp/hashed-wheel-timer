package com.piggy.spiked.timing;

public interface WaitStrategy {

    /**
     * Wait until the given deadline (in nanoseconds from epoch)
     *
     * @param deadline The deadline (in nanoseconds), until which to wait.
     * @return {@code true} if the thread has been interrupted; {@code false} if the thread
     * hasn't been interrupted
     */
    boolean waitUntil(long deadline);

    /**
     * Yielding wait strategy.
     * <p>
     * Spins in the loop, until the deadline is reached. Releases the flow control
     * by means of Thread.yield() call. This strategy is less precise than {@link BusySpinWait}
     * one, but is more scheduler-friendly.
     */
    class YieldingWait implements WaitStrategy {

        // disable the constructor
        YieldingWait() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean waitUntil(long deadline) {
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
    class BusySpinWait implements WaitStrategy {

        // disable the constructor
        BusySpinWait() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean waitUntil(long deadline) {
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
    class SleepWait implements WaitStrategy {

        // disable the constructor
        SleepWait() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean waitUntil(long deadline) {
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
