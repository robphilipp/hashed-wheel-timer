package com.piggy.spiked.timing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.piggy.spiked.timing.ScheduleType.FIXED_DELAY;
import static com.piggy.spiked.timing.ScheduleType.FIXED_RATE;

/**
 * Scheduled task that maintains its execution status state.
 *
 * @param <T> The return type of the task
 */
@SuppressWarnings("WeakerAccess")
public class ScheduledTask<T> extends CompletableFuture<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledTask.class);

    private final long id = System.nanoTime();
    private final int wheelOffset;
    private final int periodicWheelOffset;
    private final int periodicTimesAround;
    private final AtomicInteger remainingTimesAround = new AtomicInteger();
    private final Supplier<T> task;
    private final ScheduleType scheduleType;
    private final Consumer<ScheduledTask<T>> rescheduling;
    private final ExecutorService executorService;

    /**
     * @param timesAround         The number of times around the timer the cursor needs to move before the task is executed
     * @param wheelOffset         The offset in the wheel, from the current cursor, for the bucket that holds the task
     * @param periodicTimesAround The number of times around the timer the cursor needs to move before executing
     *                            the period task, after the initial delay.
     * @param periodicWheelOffset The offset in the wheel, from the current cursor, for the bucket that holds the
     *                            periodic task, after the initial delay.
     * @param timeout             The timeout for tasks on a fixed delay schedule
     * @param task                The task to execute
     * @param scheduleType        The schedule type (i.e. one-shot, fixed-rate, fixed-delay)
     * @param rescheduling        The reschedule callback that tells the hashed-wheel-timer to reschedule
     * @param executorService     The executor service for running the task
     */
    private ScheduledTask(final int timesAround,
                          final int wheelOffset,
                          final int periodicTimesAround,
                          final int periodicWheelOffset,
                          final Duration timeout,
                          final Supplier<T> task,
                          final ScheduleType scheduleType,
                          final Consumer<ScheduledTask<T>> rescheduling,
                          final ExecutorService executorService) {
        this.periodicWheelOffset = periodicWheelOffset;
        this.periodicTimesAround = periodicTimesAround;
        this.remainingTimesAround.set(timesAround);
        this.wheelOffset = wheelOffset;
        this.task = task;
        this.scheduleType = scheduleType;
        this.rescheduling = rescheduling;
        this.executorService = executorService;

        // set a timer to shut down the periodic tasks
        final ScheduledExecutorService taskCompleteExecutor = Executors.newSingleThreadScheduledExecutor();
        taskCompleteExecutor.schedule(() -> {
            complete(null);
            taskCompleteExecutor.shutdown();
            cancel(true);
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * @param <T> The return type from the task
     * @return A validated {@link ScheduledTask} instance
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * @return The wheel offset for the initial delay
     */
    public int wheelOffset() {
        return wheelOffset;
    }

    /**
     * @return The wheel offset for the periodic delay
     */
    public int periodicWheelOffset() {
        return periodicWheelOffset;
    }

    /**
     * @return The task
     */
    public Supplier<T> task() {
        return task;
    }

    /**
     * @return The schedule type (i.e. one-shot, fixed-rate, fixed-delay)
     */
    public ScheduleType scheduleType() {
        return scheduleType;
    }

    /**
     * @return whether or not the schedule has been cancelled
     */
    public boolean notCancelled() {
        return !isCancelled();
    }

    /**
     * Process the scheduled tasks. Updates the number of times around, and submits the task for execution
     * if it the cursor has gone around enough times. For periodic tasks, updates the times-around
     * and calls the hashed-while-timer to reschedule the task.
     *
     * @return A reference to this instance for periodic tasks; null for one-shot or cancelled tasks
     */
    ScheduledTask<T> process() {
        // return null for cancelled tasks
        if(isCancelled() || isDone()) {
            return null;
        }

        // todo for periodic processing need a scheduled executor to the task that cancels the timer (completes the task)
        // execute tasks that are ready
        if(remainingTimesAround.decrementAndGet() < 0) {
            switch(scheduleType) {
                case ONE_SHOT:
                    executorService.submit(() -> complete(task.get()));
                    return null;

                case FIXED_DELAY:
                    // wait for the task to complete, and then proceed to the fix-rate logic
                    task.get();
                    remainingTimesAround.set(periodicTimesAround);
                    rescheduling.accept(this);
                    return null;

                case FIXED_RATE: // approx. 40 to 60 µs on average
                    executorService.submit(task::get);
                    remainingTimesAround.set(periodicTimesAround);
                    rescheduling.accept(this);
                    return null;
            }
        }

        // do nothing and return this unchanged
        return this;
    }

    /**
     * Submits the task for execution. For periodic tasks, updates the time-around and calls the reschedule
     * function.
     *
     * @return A reference to this instance
     */
    ScheduledTask<T> executeNow() {
        executorService.submit(() -> complete(task.get()));
        switch(scheduleType) {
            case ONE_SHOT:
                return this;

            case FIXED_DELAY:
            case FIXED_RATE:
                remainingTimesAround.set(periodicTimesAround);
                rescheduling.accept(this);
                return this;
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;
        final ScheduledTask<?> that = (ScheduledTask<?>) o;
        return id == that.id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    /**
     * Builder for constructing a validated {@link ScheduledTask} instance
     *
     * @param <T> The return type of the task
     */
    public static class Builder<T> {
        private Integer timesAround;
        private Integer wheelOffset;
        private Integer periodicWheelOffset;
        private Integer periodicTimesAround;
        private Duration timeout;
        private Supplier<T> task;
        private ScheduleType scheduleType;
        private Consumer<ScheduledTask<T>> rescheduling;
        private ExecutorService executorService;

        /**
         * <p>Required for all schedule types.</p>
         * Sets the initial delay values
         *
         * @param timesAround The number of times around the timer the cursor needs to move before the task is executed
         * @param wheelOffset The offset in the wheel, from the current cursor, for the bucket that holds the task
         */
        public Builder<T> withInitialDelayInfo(final int timesAround, final int wheelOffset) {
            this.wheelOffset = wheelOffset;
            this.timesAround = timesAround;
            return this;
        }

        /**
         * <p>Required only for periodic schedules (i.e. fixed-rate, fixed-delay.</p>
         * Sets the function called when periodic tasks need to be rescheduled after executing.
         *
         * @param rescheduling The rescheduling function.
         * @return A reference to this builder for chaining
         */
        public Builder<T> withRescheduling(final Consumer<ScheduledTask<T>> rescheduling) {
            this.rescheduling = rescheduling;
            return this;
        }

        /**
         * Sets the executor service to which the tasks are submitted once the task's delay is up.
         *
         * @param executorService The executor service for running the task
         * @return A reference to this builder for chaining
         */
        public Builder<T> withExecutor(final ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        /**
         * Sets the task for a one-shot schedule (i.e. is only executed once after the delay)
         *
         * @param task The task to be executed.
         * @return A reference to this builder for chaining
         */
        public Builder<T> withOneShot(final Supplier<T> task) {
            this.task = task;
            this.scheduleType = ScheduleType.ONE_SHOT;
            return this;
        }

        /**
         * Sets the task for a periodic schedule (i.e. executed periodically after the initial delay). The task
         * is submitted for execution after a delay after previous the task has completed.
         *
         * @param task        The task to be executed.
         * @param scheduleType The type of schedule ({@link ScheduleType#FIXED_DELAY} or {@link ScheduleType#FIXED_RATE})
         * @param timesAround The number of times around the timer the cursor needs to move before executing the
         *                    period task, after the initial delay.
         * @param wheelOffset The offset in the wheel, from the current cursor, for the bucket that holds the
         *                    periodic task, after the initial delay.
         * @param timeout     The time-out for waiting for the task to finish executing
         * @return A reference to this builder for chaining
         */
        public Builder<T> withPeriodic(final Supplier<T> task,
                                       final ScheduleType scheduleType,
                                       final int timesAround,
                                       final int wheelOffset,
                                       final Duration timeout) {
            this.task = task;
            this.scheduleType = scheduleType;
            this.periodicWheelOffset = wheelOffset;
            this.periodicTimesAround = timesAround;
            this.timeout = timeout;
            return this;
        }

//        /**
//         * Sets the task for a periodic schedule (i.e. executed periodically after the initial delay). The task
//         * is submitted for execution after a delay after immediately after the previous task was submitted.
//         *
//         * @param task        The task to be executed.
//         * @param timesAround The number of times around the timer the cursor needs to move before executing the
//         *                    period task, after the initial delay.
//         * @param wheelOffset The offset in the wheel, from the current cursor, for the bucket that holds the
//         *                    periodic task, after the initial delay.
//         * @return A reference to this builder for chaining
//         */
//        public Builder<T> withFixedRate(final Supplier<T> task, final int timesAround, final int wheelOffse, final Duration timeoutt) {
//            this.task = task;
//            this.scheduleType = FIXED_DELAY;
//            this.periodicWheelOffset = wheelOffset;
//            this.periodicTimesAround = timesAround;
//            return this;
//        }

        /**
         * @return A validated {@link ScheduledTask} instance
         */
        public ScheduledTask<T> build() {
            if(Objects.isNull(timesAround) || timesAround < 0) {
                final String message = "Must specify the number of times around the wheel and that value must be non-negative";
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }
            if(Objects.isNull(wheelOffset) || wheelOffset < 0) {
                final String message = "Must specify the wheel offset and that value must be non-negative";
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }
            if(Objects.isNull(task)) {
                final String message = "Must specify the scheduled task";
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }
            if(Objects.isNull(executorService)) {
                final String message = "Must specify the executor service for task processing";
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }

            if(scheduleType == FIXED_RATE || scheduleType == FIXED_DELAY) {
                if(Objects.isNull(periodicTimesAround) || periodicTimesAround < 0) {
                    final String message = "Must specify the number of times around the wheel for periodic schedule and that value must be non-negative";
                    LOGGER.error(message);
                    throw new IllegalStateException(message);
                }
                if(Objects.isNull(periodicWheelOffset) || periodicWheelOffset < 0) {
                    final String message = "Must specify the wheel offset for periodic schedule and that value must be non-negative";
                    LOGGER.error(message);
                    throw new IllegalStateException(message);
                }
                if(scheduleType == FIXED_DELAY && (Objects.isNull(timeout) || timeout.isNegative() || timeout.isZero())) {
                    final String message = String.format(
                            "Fixed delay schedules require a task time-out that is a positive value; timeout: %,d µs",
                            timeout.toNanos() * 1000
                    );
                    LOGGER.error(message);
                    throw new IllegalStateException(message);
                }
                if(Objects.isNull(rescheduling)) {
                    final String message = "Must specify the rescheduling callback";
                    LOGGER.error(message);
                    throw new IllegalStateException(message);
                }
            } else {
                periodicWheelOffset = -1;
                periodicTimesAround = -1;
            }
            return new ScheduledTask<>(
                    timesAround, wheelOffset,
                    periodicTimesAround, periodicWheelOffset,
                    timeout, task, scheduleType, rescheduling, executorService
            );
        }
    }
}
