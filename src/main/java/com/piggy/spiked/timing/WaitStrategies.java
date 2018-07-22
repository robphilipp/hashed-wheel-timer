package com.piggy.spiked.timing;

public class WaitStrategies {
    /**
     * BusySpin wait strategy.
     * <p>Current implementation has a resolution of approximately 5 µs but can take
     * as long as 25 µs to do one loop.</p>
     * <p>
     * Spins in the loop until the deadline is reached. In a multi-core environment,
     * will occupy an entire core. Is more precise than Sleep wait strategy, but
     * consumes more resources.
     */
    static WaitStrategy.BusySpinWait busySpinWait() {
        return new WaitStrategy.BusySpinWait();
    }

    /**
     * Yielding wait strategy.
     * <p>
     * Spins in the loop, until the deadline is reached. Releases the flow control
     * by means of Thread.yield() call. This strategy is less precise than BusySpin
     * one, but is more scheduler-friendly.
     */
    static WaitStrategy.YieldingWait yieldingWait() {
        return new WaitStrategy.YieldingWait();
    }

    /**
     * Sleep wait strategy.
     * <p>
     * Will release the flow control, giving other threads a possibility of execution
     * on the same processor. Uses less resources than BusySpin wait, but is less
     * precise.
     */
    static WaitStrategy.SleepWait sleepWait() {
        return new WaitStrategy.SleepWait();
    }
}
