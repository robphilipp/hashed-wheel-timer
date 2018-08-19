package com.piggy.spiked.timing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Original code contained the following header:<p>
 * Hash Wheel Timer, as per the paper:
 * <p>
 * Hashed and hierarchical timing wheels:
 * http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf
 * <p>
 * More comprehensive slides, explaining the paper can be found here:
 * http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt
 * <p>
 * Hash Wheel timer is an approximated timer that allows performant execution of
 * larger amount of tasks with better performance compared to traditional scheduling.
 *
 * @author Oleksandr Petrov
 *
 * Updated hashed-wheel-timer that supports reasonable sub-millisecond delays (with
 * delays down to about 100 µs with 50 µs resolution). The basic approach is that
 * of the original code, but much of it has been rewritten in an attempt to get
 * the sub-millisecond delays with reasonable presicsion and accuracy.
 *
 * To construct a hashed-wheel-timer, use the {@link HashedWheelTimer.Builder}. And
 * once constructed, call {@link HashedWheelTimer#start()} to start the timer
 * processing loop. Once done with the timer, call the {@link HashedWheelTimer#shutdown()}
 * or {@link HashedWheelTimer#shutdownNow()} method.
 *
 * @author Rob Philipp
 */
@SuppressWarnings("WeakerAccess")
public class HashedWheelTimer {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashedWheelTimer.class);

    private final int wheelSize;
    private final long resolution;
    private final ExecutorService loop;
    private final ExecutorService executor;
    private final WaitStrategy waitStrategy;

    private final ConcurrentMap<Integer, ConcurrentSkipListSet<ScheduledTask<?>>> wheel;
    private final AtomicInteger cursor = new AtomicInteger(0);

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Create a new {@code HashedWheelTimer} using the given timer resolution and wheelSize. All times will
     * rounded up to the closest multiple of this resolution.
     * @param name       name for daemon thread factory to be displayed
     * @param resolution resolution of this timer in NANOSECONDS
     * @param wheelSize  size of the Ring Buffer supporting the Timer, the larger the wheel, the less the lookup time is
     *                   for sparse timeouts. Sane default is 512.
     * @param strategy   waitStrategy for waiting for the next tick
     * @param executor   Executor instance to submit tasks to once the scheduled time has been hit
     */
    private HashedWheelTimer(final String name,
                             final long resolution,
                             final int wheelSize,
                             final WaitStrategy strategy,
                             final ExecutorService executor) {
        this.waitStrategy = strategy;
        this.wheelSize = wheelSize;
        this.resolution = resolution;
        this.executor = executor;

        // create the timer wheel and the set holding the scheduled tasks
        this.wheel = new ConcurrentHashMap<>(wheelSize);
        IntStream
                .range(0, wheelSize)
                .forEach(bucket -> wheel.putIfAbsent(
                        bucket,
                        new ConcurrentSkipListSet<>(Comparator.comparingInt(ScheduledTask::wheelOffset))
                ));

        // sets the timer-executor service in which the timer's update loop runs
        this.loop = timerExecutorService(name);
    }

    /**
     * @return Builder for constructing a validated {@link HashedWheelTimer} instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a single-thread executor for running the loop
     *
     * @param timerName The name of the timer
     * @return a single-thread executor with a factory to name the threads
     */
    private ExecutorService timerExecutorService(final String timerName) {
        return Executors.newSingleThreadExecutor(new ThreadFactory() {
            final AtomicInteger numThreads = new AtomicInteger();

            /**
             * Creates a new thread for the runnable
             * @param runnable The runnable
             * @return a new thread for the runnable
             */
            @Override
            public Thread newThread(final Runnable runnable) {
                final Thread thread = new Thread(runnable, timerName + "-" + numThreads.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    /**
     * Starts the hashed-wheel-timer's timer loop
     * @return A reference to this instance for chaining
     */
    public HashedWheelTimer start() {
        loop.submit(timerLoop());
        return this;
    }

    /**
     * Attempts to gracefully shut-down the timer loop and any executing processes
     */
    public void shutdown() {
        loop.shutdown();
        executor.shutdown();
    }

    /**
     * Attempts to shut down the timer loop and any executing processes
     * @return A list of tasks that were halted as a result of this call.
     */
    public List<Runnable> shutdownNow() {
        loop.shutdownNow();
        return executor.shutdownNow();
    }

    /**
     * Blocks until all scheduled tasks and timer-loop tasks have completed execution after a shutdown
     * request, or the timeout occurs, or the current thread is interrupted, whichever happens first.
     * @param timeout The amount of time to wait
     * @param unit The unit of time association with the timeout
     * @return {@code true} if the schedule task executor and the timer-loop executor have terminated;
     * {@code false} otherwise
     * @throws InterruptedException If interrupted while waiting
     */
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        return loop.awaitTermination(timeout, unit) && executor.awaitTermination(timeout, unit);
    }

    /**
     * @return {@code true} if the timer-loop and scheduled-task executor are shut down; {@code false} otherwise
     */
    public boolean isShutdown() {
        return loop.isShutdown() && executor.isShutdown();
    }

    /**
     * @return {@code true} if the timer-loop and scheduled-task executor are terminated; {@code false} otherwise
     */
    public boolean isTerminated() {
        return loop.isTerminated() && executor.isTerminated();
    }

    /**
     * Returns a runnable function that updates the cursor, runs processes at their scheduled time, and
     * calls the wait function before processing the next bucket in the wheel. The loop is exited when the
     * {@link WaitStrategy#waitUntil(long)} method returns an interrupted flag.
     *
     * @return A timer-loop function
     */
    private Runnable timerLoop() {
        return () -> {
            long deadline = System.nanoTime();
            boolean interrupted;
            do {
                // grab and update the current cursor
                lock.lock();
                final int currentCursor = cursor.getAndUpdate(val -> (val + 1) % wheelSize);

                // update the scheduled tasks
                wheel.computeIfPresent(wheelIndex(currentCursor), (index, scheduledTasks) -> {
                    // update the registration list based on the results of the call to process the task
                    // the task.process does the following:
                    // 1. executes the task if ready
                    // 2. reschedules the task after processing for periodic tasks,
                    // and upon completion, returns
                    // 1. null if cancelled or after one-shot has executed
                    // 2. the task if rescheduled
                    // All the tasks that are set to null (i.e. cancelled or one-shot) are removed from
                    // the list.
                    scheduledTasks.removeIf(task -> Objects.isNull(task.process()));  // < 1 µs with no tasks, approx. 40 to 80 µs with tasks
                    return scheduledTasks;
                });
                lock.unlock();

                // updates the deadline for the registrations in the next wheel bucket, and provides a
                // catch-up in case the registration updates take more than the resolution
                final long currentTime = System.nanoTime();
                deadline = Math.max(deadline + resolution, currentTime);

                // wait for the deadline if it isn't equal to the current time
                interrupted = deadline != currentTime && waitStrategy.waitUntil(deadline);
            } while (!interrupted);
        };
    }

    /**
     * Schedule a task to run after the specified delay
     * @param task The task to run (called when the delay is over)
     * @param delay The amount of time to delay
     * @param timeUnit The unit associated with the delay time
     * @param <V> The return type from the task
     * @return A future that will hold the task result upon completion
     */
    public <V> CompletableFuture<V> schedule(final Supplier<V> task, final long delay, final TimeUnit timeUnit) {
        return scheduleOneShot(TimeUnit.NANOSECONDS.convert(delay, timeUnit), task);
    }

    /**
     * Schedules the task at a fixed rate (i.e. doesn't wait for the task to complete). Will attempt to
     * run the tasks periodically with the specified period.
     * @param task The task to be executed
     * @param timeout The duration of the timer
     * @param timeoutUnits The units of the timer duration
     * @param initialDelay The initial delay
     * @param period The periodic delay
     * @param unit The time unit associated with the delay and the period
     * @param <V> The return type of the task
     * @return The future holding for the task
     */
    public <V> CompletableFuture<V> scheduleAtFixedRate(final Supplier<V> task,
                                                        final long timeout,
                                                        final TimeUnit timeoutUnits,
                                                        final long initialDelay,
                                                        final long period,
                                                        final TimeUnit unit) {
        return schedulePeriodic(
                ScheduleType.FIXED_RATE,
                TimeUnit.NANOSECONDS.convert(initialDelay, unit),
                TimeUnit.NANOSECONDS.convert(period, unit),
                Duration.ofNanos(TimeUnit.NANOSECONDS.convert(timeout, timeoutUnits)),
                task
        );
    }

    /**
     * Schedule a task executed after an initial delay and then repeatedly with a delay of {@code period}
     * after the tasks has completed execution.
     * @param task The task to be executed
     * @param timeout The duration of the timer
     * @param timeoutUnits The units of the timer duration
     * @param initialDelay The initial delay
     * @param period The periodic delay
     * @param unit The time unit associated with the delay and the period
     * @param <V> The return type of the task
     * @return The future holding for the task
     */
    public <V> CompletableFuture<V> scheduleWithFixedDelay(final Supplier<V> task,
                                                           final long timeout,
                                                           final TimeUnit timeoutUnits,
                                                           final long initialDelay,
                                                           final long period,
                                                           final TimeUnit unit) {
        return schedulePeriodic(
                ScheduleType.FIXED_DELAY,
                TimeUnit.NANOSECONDS.convert(initialDelay, unit),
                TimeUnit.NANOSECONDS.convert(period, unit),
                Duration.ofNanos(TimeUnit.NANOSECONDS.convert(timeout, timeoutUnits)),
                task
        );
    }

//    /**
//     * Create a wrapper Function, which will "debounce" i.e. postpone the function execution until after <code>period</code>
//     * has elapsed since last time it was invoked. <code>delegate</code> will be called most once <code>period</code>.
//     *
//     * @param delegate delegate runnable to be wrapped
//     * @param period   given time period
//     * @param timeUnit unit of the period
//     * @return wrapped runnable
//     */
//    public Runnable debounce(Runnable delegate,
//                             long period,
//                             TimeUnit timeUnit) {
//        AtomicReference<ScheduledFuture<?>> reg = new AtomicReference<>();
//
//        return () -> {
//            ScheduledFuture<?> future = reg.getAndSet(scheduleOneShot(TimeUnit.NANOSECONDS.convert(period, timeUnit),
//                    () -> {
//                        delegate.run();
//                        return null;
//                    }));
//            if (future != null) {
//                future.cancel(true);
//            }
//        };
//    }
//
//    /**
//     * Create a wrapper Consumer, which will "debounce" i.e. postpone the function execution until after <code>period</code>
//     * has elapsed since last time it was invoked. <code>delegate</code> will be called most once <code>period</code>.
//     *
//     * @param delegate delegate consumer to be wrapped
//     * @param period   given time period
//     * @param timeUnit unit of the period
//     * @return wrapped runnable
//     */
//    public <T> Consumer<T> debounce(Consumer<T> delegate,
//                                    long period,
//                                    TimeUnit timeUnit) {
//        AtomicReference<ScheduledFuture<T>> reg = new AtomicReference<>();
//
//        return t -> {
//            ScheduledFuture<T> future = reg.getAndSet(scheduleOneShot(TimeUnit.NANOSECONDS.convert(period, timeUnit),
//                    () -> {
//                        delegate.accept(t);
//                        return t;
//                    }));
//            if (future != null) {
//                future.cancel(true);
//            }
//        };
//    }
//
//    /**
//     * Create a wrapper Runnable, which creates a throttled version, which, when called repeatedly, will call the
//     * original function only once per every <code>period</code> milliseconds. It's easier to think about throttle
//     * in terms of it's "left bound" (first time it's called within the current period).
//     *
//     * @param delegate delegate runnable to be called
//     * @param period   period to be elapsed between the runs
//     * @param timeUnit unit of the period
//     * @return wrapped runnable
//     */
//    public Runnable throttle(final Runnable delegate, final long period, final TimeUnit timeUnit) {
//        final AtomicBoolean alreadyWaiting = new AtomicBoolean();
//
//        return () -> {
//            if (alreadyWaiting.compareAndSet(false, true)) {
//                scheduleOneShot(TimeUnit.NANOSECONDS.convert(period, timeUnit),
//                        () -> {
//                            delegate.run();
//                            alreadyWaiting.compareAndSet(true, false);
//                            return null;
//                        });
//            }
//        };
//    }
//
//    /**
//     * Create a wrapper Consumer, which creates a throttled version, which, when called repeatedly, will call the
//     * original function only once per every <code>period</code> milliseconds. It's easier to think about throttle
//     * in terms of it's "left bound" (first time it's called within the current period).
//     *
//     * @param delegate delegate consumer to be called
//     * @param period   period to be elapsed between the runs
//     * @param timeUnit unit of the period
//     * @return wrapped runnable
//     */
//    public <T> Consumer<T> throttle(final Consumer<T> delegate, final long period, final TimeUnit timeUnit) {
//        final AtomicBoolean alreadyWaiting = new AtomicBoolean();
//        final AtomicReference<T> lastValue = new AtomicReference<>();
//
//        return val -> {
//            lastValue.set(val);
//            if (alreadyWaiting.compareAndSet(false, true)) {
//                scheduleOneShot(TimeUnit.NANOSECONDS.convert(period, timeUnit),
//                        () -> {
//                            delegate.accept(lastValue.getAndSet(null));
//                            alreadyWaiting.compareAndSet(true, false);
//                            return null;
//                        });
//            }
//        };
//    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("HashedWheelTimer { Buffer Size: %d, Resolution: %d }",
                wheelSize,
                resolution);
    }

    /**
     * Calculates the index in the wheel given the cursor and the wheel size
     * @param cursor The current cursor position
     * @return The index into the wheel (accounting for the fact that the cursor may
     * be larger than the wheel size).
     */
    private int wheelIndex(final int cursor) {
        return cursor % wheelSize;
    }

    /**
     * Calculates the bucket in the wheel that the specified delay duration falls
     * @param duration The duration in nanoseconds
     * @return The index of the bucket into which the duration falls
     */
    private int wheelOffset(final long duration) {
        return (int) Math.max(0, (duration / resolution));
    }

    /**
     * Calculates the number of times around the wheel a scheduled delay represents. For example,
     * suppose that the resolution is 1 ms and there are 32 buckets. Then once around the wheel
     * represents a wait of 32 ms. A delay of 67 ms would have an offset of 67, which would  be in
     * the third bucket after the cursor moved around the bucket twice.
     * @param wheelOffset The wheel offset given by the {@link #wheelOffset(long)} method
     * @return The number of times-around the wheel for the specified offset.
     */
    private int wheelPeriods(final int wheelOffset) {
        return wheelOffset / wheelSize;
    }

    /**
     * Asserts that the timer loop is running and that the first delay is not smaller than the
     * resolution.
     * @param firstDelay The first delay.
     */
    private void assertRunningAndValid(final long firstDelay) {
        // ensure that the timer is still running and that the delay is greater or equal to the resulotion
        if (loop.isTerminated()) {
            final String message = "Cannot schedule tasks when the hash-wheel-timer is not longer running.";
            LOGGER.error(message);
            throw new IllegalStateException(message);
        }
        if (firstDelay < resolution) {
            final String message = String.format(
                    "Schedule delay must be greater than or equal to the timer resolution; delay: %,d ns; resolution: %,d ns.",
                    firstDelay, resolution
            );
            LOGGER.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Schedules a one-shot delay
     *
     * @param firstDelay The delay in nanoseconds
     * @param task       The supplier that is run when the delay expires
     * @param <V>        The return type of the task
     * @return The completion future that is completed once the task is run
     */
    private <V> CompletableFuture<V> scheduleOneShot(final long firstDelay, final Supplier<V> task) {
        // grab the start time so that we can correct the delay for the amount of time it took to
        // instantiate the scheduled tasks
        final long start = System.nanoTime();

        // ensure that the timer is still running and that the delay is greater or equal to the resolution
        assertRunningAndValid(firstDelay);

        // the number of buckets required to cover the delay
        final int firstFireOffset = wheelOffset(firstDelay);

        // the number of times around the wheel before the delay is over
        final int firstFireRounds = wheelPeriods(firstFireOffset);

        // create the new tasks, and also get a rough estimate of the time it took to instantiate the task
        final ScheduledTask<V> scheduledTask = ScheduledTask.<V>builder()
                .withInitialDelayInfo(firstFireRounds, firstFireOffset)
                .withExecutor(executor)
                .withOneShot(task)
                .build();
        final long instantiationTime = System.nanoTime() - start;
        if (firstDelay - instantiationTime <= 0) {
            System.out.println(String.format("Completed immediately; instantiation time: %,d ns", instantiationTime));
            return scheduledTask.executeNow();
        }

        // calculate the wheel offset after subtraction the instantiation time from the request delay
        final int adjustedOffset = wheelOffset(firstDelay - instantiationTime);

        // add the registration to the bucket the will be processed next. the cursor is always set to the
        // bucket the will be processed next.
        lock.lock();
        wheel.computeIfPresent(wheelIndex(cursor.get() + adjustedOffset), (index, registrations) -> {
            registrations.add(scheduledTask);
            return registrations;
        });
        lock.unlock();
        return scheduledTask;
    }

//    private <V> Registration<V> scheduleFixedRate(long recurringTimeout,
//                                                  long firstDelay,
//                                                  Callable<V> callable) {
//        assertRunning();
//        isTrue(recurringTimeout >= resolution,
//                "Cannot schedule tasks for amount of time less than timer precision.");
//
//        int offset = (int) (recurringTimeout / resolution);
//        int rounds = offset / wheelSize;
//
//        int firstFireOffset = (int) (firstDelay / resolution);
//        int firstFireRounds = firstFireOffset / wheelSize;
//
//        Registration<V> r = new FixedRateRegistration<>(firstFireRounds, callable, recurringTimeout, rounds, offset);
////        wheel[wheelIndex(cursor + firstFireOffset + 1)].add(r);
//        wheel.get(wheelIndex(cursor.get() + firstFireOffset + 1)).add(r);
//        return r;
//    }
//

    /**
     * Schedules the specified task to be executed after an initial delay, then periodically, after the
     * task has completed, at specified periodic delay.
     * @param firstDelay The first delay (in nanoseconds) after which to execute the task
     * @param periodicDelay The periodic delay (in nanoseconds) after the task has completed, to execute the task
     * @param timeout The duration of the periodic timer (time until it stops)
     * @param task The task to execute
     * @param <V> The return type of the task
     * @return The completable future with the result.
     */
    private <V> CompletableFuture<V> schedulePeriodic(final ScheduleType scheduleType,
                                                      final long firstDelay,
                                                      final long periodicDelay,
                                                      final Duration timeout,
                                                      final Supplier<V> task) {
        // grab the start time so that we can correct the delay for the amount of time it took to
        // instantiate the scheduled tasks
        final long start = System.nanoTime();

        // ensure that the timer is still running and that the delay is greater or equal to the resolution
        assertRunningAndValid(firstDelay);

        // the number of buckets required to cover the delay
        final int firstFireOffset = wheelOffset(firstDelay);
        final int periodicFireOffset = wheelOffset(periodicDelay);

        // the number of times around the wheel before the delay is over
        final int firstFireRounds = wheelPeriods(firstFireOffset);
        final int periodicTimeAround = wheelPeriods(periodicFireOffset);

        // create the new tasks, and also get a rough estimate of the time it took to instantiate the task
        final ScheduledTask<V> scheduledTask = ScheduledTask.<V>builder()
                .withInitialDelayInfo(firstFireRounds, firstFireOffset)
                .withRescheduling(this::reschedule)
                .withExecutor(executor)
                .withPeriodic(task, scheduleType, periodicTimeAround, periodicFireOffset, timeout)
                .build();

        final long instantiationTime = System.nanoTime() - start;
        if (firstDelay - instantiationTime <= 0) {
            System.out.println(String.format("Completed immediately; instantiation time: %,d ns", instantiationTime));
            return scheduledTask.executeNow();
        }

        // calculate the wheel offset after subtraction the instantiation time from the request delay
        final int adjustedOffset = wheelOffset(firstDelay - instantiationTime);

        // add the registration to the bucket the will be processed next. the cursor is always set to the
        // bucket the will be processed next. note that when the wheel is set up, a concurrent skip list is
        // added to each bucket, and so the an index will always be present
        lock.lock();
        wheel.computeIfPresent(
                wheelIndex(cursor.get() + adjustedOffset),
                (index, registrations) -> {
                    registrations.add(scheduledTask);
                    return registrations;
                });
        lock.unlock();
        return scheduledTask;
    }

    /**
     * Reschedule a periodic task for that has completed to execute the tasks at the next
     * delay.
     *
     * @param registration The registration to reschedule
     */
    private void reschedule(final ScheduledTask<?> registration) {
        lock.lock();
        wheel.get(wheelIndex(cursor.get() + registration.periodicWheelOffset() + 1)).add(registration);
        lock.unlock();
    }

//    private void assertRunning() {
//        if (loop.isTerminated()) {
//            throw new IllegalStateException("Timer is not running");
//        }
//    }

//    private static void isTrue(boolean expression, String message) {
//        if (!expression) {
//            throw new IllegalArgumentException(message);
//        }
//    }

//    private static Callable<?> constantlyNull(Runnable r) {
//        return () -> {
//            r.run();
//            return null;
//        };
//    }

    /**
     * Builder for constructing validated {@link HashedWheelTimer} instances
     */
    @SuppressWarnings({"unused", "WeakerAccess"})
    public static class Builder {
        private static final String DEFAULT_TIMER_NAME = "hashed-wheel-timer";
        private static final long DEFAULT_RESOLUTION = 10;
        private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
        private static final int DEFAULT_WHEEL_SIZE = 512;

        private String timerName;
        private Long resolution;
        private TimeUnit units;
        private Integer wheelSize;
        private WaitStrategy waitStrategy;
        private ExecutorService executor;

        /**
         * @param name name for daemon thread factory to be displayed
         * @return a reference to this builder for chaining
         */
        public Builder withTimerName(final String name) {
            this.timerName = name;
            return this;
        }

        /**
         * Sets the default name ({@value DEFAULT_TIMER_NAME}) for the timer's thread factory
         * @return a reference to this builder for chaining
         */
        public Builder withDefaultTimerName() {
            this.timerName = DEFAULT_TIMER_NAME;
            return this;
        }

        /**
         * Sets the time-resolution for the timer. Generally, this is the most precise the timer will be.
         * For example, setting the resolution to 1 and the time units to milliseconds, means that the
         * timer, in best case, is precise to 1 ms.
         * @param resolution resolution of this timer
         * @param units The time-units for the resolution
         * @return a reference to this builder for chaining
         */
        public Builder withResolution(final long resolution, final TimeUnit units) {
            this.resolution = resolution;
            this.units = units;
            return this;
        }

        /**
         * Sets the default resolution ({@value DEFAULT_RESOLUTION} {@value DEFAULT_TIME_UNIT}) for the timer
         * @return a reference to this builder for chaining
         */
        public Builder withDefaultResolution() {
            this.resolution = DEFAULT_RESOLUTION;
            this.units = DEFAULT_TIME_UNIT;
            return this;
        }

        /**
         * Sets the number of buckets in the hash-wheel.
         * @param numBuckets The number of buckets in the hash-wheel
         * @return a reference to this builder for chaining
         */
        public Builder withWheelSize(final int numBuckets) {
            this.wheelSize = numBuckets;
            return this;
        }

        /**
         * Sets the default wheel size ({@value DEFAULT_WHEEL_SIZE}) for the timer
         * @return a reference to this builder for chaining
         */
        public Builder withDefaultWheelSize() {
            this.wheelSize = DEFAULT_WHEEL_SIZE;
            return this;
        }

        /**
         * Sets how wait-time between successive buckets is implemented. For example,
         * {@link WaitStrategy.BusySpinWait} loops through successive calls to {@link System#nanoTime()}
         * until the deadline has been reached to move to the next bucket.
         * @param strategy The wait strategy
         * @return a reference to this builder for chaining
         */
        public Builder withWaitStrategy(final WaitStrategy strategy) {
            this.waitStrategy = strategy;
            return this;
        }

        /**
         * Sets the executor that is used to process the time loop.
         * @param executor The timer-loop executor
         * @return a reference to this builder for chaining
         */
        public Builder withExecutor(final ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Sets the default executor ({@link Executors#newFixedThreadPool(int)}) with one thread.
         * @return a reference to this builder for chaining
         */
        public Builder withDefaultExecutor() {
            this.executor = Executors.newFixedThreadPool(1);
            return this;
        }

        /**
         * Sets the default executor ({@link Executors#newFixedThreadPool(int)}) with one thread.
         * @param numThreads The number of threads for the executor
         * @return a reference to this builder for chaining
         */
        public Builder withDefaultExecutor(final int numThreads) {
            this.executor = Executors.newFixedThreadPool(numThreads);
            return this;
        }

        /**
         * @return A validated {@link HashedWheelTimer} instance
         */
        public HashedWheelTimer build() {
            if (Objects.isNull(timerName) || timerName.isEmpty()) {
                final String message = "Timer name must be specified and cannot be empty";
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }
            if (Objects.isNull(resolution) || resolution <= 0) {
                final String message = "Timer resolution must be specified and must be a positive number";
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }
            if (Objects.isNull(units)) {
                final String message = "Units for the timer resolution must be specified and cannot be null";
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }
            if (TimeUnit.MICROSECONDS.convert(resolution, units) < 50) {
                final String message = String.format("Timer resolution must be 50 µs or greater; specified: %d %s", resolution, units.toString());
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }
            if (Objects.isNull(waitStrategy)) {
                final String message = "Timer wait strategy must be specified and cannot be null";
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }
            if (Objects.isNull(executor)) {
                final String message = "Executor for the timer loop must be specified and cannot be null; or use withDefaultExecutor() method.";
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }
            final long resolutionNanos = TimeUnit.NANOSECONDS.convert(resolution, units);
            return new HashedWheelTimer(timerName, resolutionNanos, wheelSize, waitStrategy, executor);
        }
    }
}
