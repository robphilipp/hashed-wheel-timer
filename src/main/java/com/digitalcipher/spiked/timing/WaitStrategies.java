package com.digitalcipher.spiked.timing;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

/**
 * Factory methods for creating {@link WaitStrategy} instances.
 * <p>
 * The wait strategies. Busy-spin and yielding wait seem to have about the same accuracy,
 * though, busy-spin may be a tad more accurate at short (~200 µs) delay times. Sleep
 * wait should really only be used for timer-resolutions of 10 ms or greater.
 * </p>
 */
public class WaitStrategies {

    /**
     * Returns a new strategy for the corresponding string-literal strategy name
     * @param strategyLiteral The wait strategy as a string
     * @return An {@link Optional} holding the {@link WaitStrategy} corresponding to the
     * specified strategy name; or an empty {@link Optional} if the strategy name was
     * invalid.
     */
    public static Optional<WaitStrategy> from(final String strategyLiteral) {
        return Strategy.from(strategyLiteral).map(Strategy::create);
    }

    /**
     * BusySpin wait strategy.
     * <p>Current implementation has a resolution of approximately 5 µs but can take
     * as long as 25 µs to do one loop.</p>
     * <p>
     * Spins in the loop until the deadline is reached. In a multi-core environment,
     * will occupy an entire core. Is more precise than Sleep wait strategy, but
     * consumes more resources.
     * @return A {@link WaitStrategy.BusySpinWait} instances
     */
    public static WaitStrategy.BusySpinWait busySpinWait() {
        return new WaitStrategy.BusySpinWait();
    }

    /**
     * Yielding wait strategy.
     * <p>
     * Spins in the loop, until the deadline is reached. Releases the flow control
     * by means of Thread.yield() call. This strategy is less precise than BusySpin
     * one, but is more scheduler-friendly.
     * @return A {@link WaitStrategy.YieldingWait} instances
     */
    public static WaitStrategy.YieldingWait yieldingWait() {
        return new WaitStrategy.YieldingWait();
    }

    /**
     * Sleep wait strategy.
     * <p>
     * Will release the flow control, giving other threads a possibility of execution
     * on the same processor. Uses less resources than BusySpin wait, but is less
     * precise.
     * @return A {@link WaitStrategy.SleepWait} instances
     */
    public static WaitStrategy.SleepWait sleepWait() {
        return new WaitStrategy.SleepWait();
    }

    /**
     * An enumeration factory for the various wait strategies
     */
    public enum Strategy {
        BUSY_SPIN("busy_spin", strategy -> busySpinWait()),
        YIELDING_WAIT("yielding_wait", strategy -> yieldingWait()),
        SLEEP_WAIT("sleep_wait", strategy -> sleepWait())
        ;


        private final String literal;
        private final Function<Strategy, WaitStrategy> factory;

        /**
         * @param literal The string literal represenatation of the strategy
         * @param factory The factory function to create the strategy
         */
        Strategy(final String literal, final Function<Strategy, WaitStrategy> factory) {
            this.literal = literal;
            this.factory = factory;
        }

        /**
         * @return The string literal for the strategy
         */
        public String literal() {
            return literal;
        }

        /**
         * Invokes the factory function for the strategy
         * @return A new instance of the strategy
         */
        public WaitStrategy create() {
            return factory.apply(this);
        }

        /**
         * @param literal The string literal name of the strategy
         * @return An optional holding the corresponding {@link Strategy} enumeration; or an empty optional
         * if the strategy literal was invalid
         */
        public static Optional<Strategy> from(final String literal) {
            return Arrays.stream(values()).filter(value -> value.literal.equalsIgnoreCase(literal)).findAny();
        }
    }

}
