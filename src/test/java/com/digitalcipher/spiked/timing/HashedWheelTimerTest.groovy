package com.digitalcipher.spiked.timing

import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * Basic tests for the timers. For benchmarks, see
 * <a href="https://github.com/robphilipp/hashed-wheel-timer-benchmarks">the benchmark repository</a>.
 */
class HashedWheelTimerTest extends Specification {

    /**
     * Creates a hashed-wheel timer with specified resolution, wheel-size, and wait strategy
     * @param resolution The time-interval for each wheel bucket
     * @param wheelSize The number of buckets in the wheel
     * @param waitStrategy The strategy employed for waiting to advance the cursor
     * @return The timer
     */
    def timer(Duration resolution, int wheelSize, WaitStrategy waitStrategy, int executorThreads = 1) {
        def timer = HashedWheelTimer.builder()
                .withDefaultTimerName()
                .withDefaultExecutor(executorThreads)
                .withResolution(resolution.toNanos(), TimeUnit.NANOSECONDS)
                .withWheelSize(wheelSize)
                .withWaitStrategy(waitStrategy)
                .build()

        return timer
    }

    def units(TimeUnit unit) {
        switch (unit) {
            case TimeUnit.NANOSECONDS: return "ns"
            case TimeUnit.MICROSECONDS: return "µs"
            case TimeUnit.MILLISECONDS: return "ms"
            case TimeUnit.SECONDS: return "s"
            default: return ""
        }
    }

    def timeFrom(long nanos, TimeUnit unit) {
        return "${unit.convert(nanos, TimeUnit.NANOSECONDS)} ${units(unit)}"
    }

    def "should be able to schedule a one-shot task"() {
        setup: "create, start, and prime the timer"
        def timer = timer(Duration.ofMillis(1), 512, WaitStrategies.busySpinWait()).start()

        def delay = 10
        def units = TimeUnit.MILLISECONDS
        def primeStart = System.nanoTime()
        println((timer.schedule({ -> System.nanoTime() }, delay, units).get() - primeStart) / 1e6 + " ms")
        primeStart = System.nanoTime()
        println((timer.schedule({ -> System.nanoTime() }, delay, units).get() - primeStart) / 1e6 + " ms")

        when: "we schedule a task with a 10 ms delay"
        def start = System.nanoTime()
        def executedTime = timer.schedule({ -> System.nanoTime() }, delay, TimeUnit.MILLISECONDS).get()

        and: "and calculate the actual delay"
        def actualDelay = executedTime - start

        then: "the actual delay should be at least the specified delay"
        actualDelay > TimeUnit.NANOSECONDS.convert(delay, units)

        cleanup: "shutdown the timer"
        timer.shutdown()
    }

    def "should be able to schedule a one-shot task with a sleep wait strategy"() {
        setup: "create, start, and prime the timer"
        def timer = timer(Duration.ofMillis(10), 512, WaitStrategies.sleepWait()).start()

        def delay = 1000
        def units = TimeUnit.MILLISECONDS

        when: "we schedule a task with a 10 ms delay"
        def start = System.nanoTime()
        def executedTime = timer.schedule({ -> System.nanoTime() }, delay, TimeUnit.MILLISECONDS).get()

        and: "and calculate the actual delay"
        def actualDelay = executedTime - start

        then: "the actual delay should be at least the specified delay"
        actualDelay > TimeUnit.NANOSECONDS.convert(delay, units)

        cleanup: "shutdown the timer"
        timer.shutdown()
    }

    @Unroll
    "one-shot task with delay of #delay and resolution #resolution should have approximate delay"() {
        setup: "create, start, and prime the timer"
        def timerResolution = Duration.ofNanos(TimeUnit.NANOSECONDS.convert(Resolution, ResolutionUnits))
        def timer = timer(timerResolution, WheelSize, WaitStrategies.busySpinWait()).start()

        // prime the timer
        timer.schedule({ -> System.nanoTime() }, Delay, DelayUnits).get()
        timer.schedule({ -> System.nanoTime() }, Delay, DelayUnits).get()

        expect: "we schedule a task with a 10 ms delay"
        def start = System.nanoTime()
        def executedTime = timer.schedule({ -> System.nanoTime() }, Delay, DelayUnits).get()

        and: "and calculate the actual delay"
        def actualDelay = executedTime - start

        and: "the actual delay should be at least the specified delay"
        DelayUnits.convert(actualDelay, TimeUnit.NANOSECONDS) >= Delay - Resolution

        and:
        timer.shutdown()

        where:
        Resolution | ResolutionUnits       | Delay | DelayUnits            | WheelSize
        200        | TimeUnit.MICROSECONDS | 200   | TimeUnit.MICROSECONDS | 512
        200        | TimeUnit.MICROSECONDS | 200   | TimeUnit.MICROSECONDS | 512
        200        | TimeUnit.MICROSECONDS | 200   | TimeUnit.MICROSECONDS | 512
        200        | TimeUnit.MICROSECONDS | 200   | TimeUnit.MICROSECONDS | 512
        200        | TimeUnit.MICROSECONDS | 200   | TimeUnit.MICROSECONDS | 512
        200        | TimeUnit.MICROSECONDS | 200   | TimeUnit.MICROSECONDS | 512
        200        | TimeUnit.MICROSECONDS | 200   | TimeUnit.MICROSECONDS | 512
        200        | TimeUnit.MICROSECONDS | 200   | TimeUnit.MICROSECONDS | 512
        200        | TimeUnit.MICROSECONDS | 200   | TimeUnit.MICROSECONDS | 512

        200        | TimeUnit.MICROSECONDS | 1     | TimeUnit.MILLISECONDS | 512
        200        | TimeUnit.MICROSECONDS | 1     | TimeUnit.MILLISECONDS | 512
        200        | TimeUnit.MICROSECONDS | 1     | TimeUnit.MILLISECONDS | 512
        200        | TimeUnit.MICROSECONDS | 1     | TimeUnit.MILLISECONDS | 512
        1          | TimeUnit.MILLISECONDS | 10    | TimeUnit.MILLISECONDS | 512

        resolution = "${Resolution} ${units(ResolutionUnits)}"
        delay = "${Delay} ${units(DelayUnits)}"
    }

    @Unroll
    "should be able to run multiple one-shot tasks with delay of #delay and resolution #resolution"() {
        setup: "create, start, and prime the timer"
        def timerResolution = Duration.ofNanos(TimeUnit.NANOSECONDS.convert(Resolution, ResolutionUnits))
        def timer = timer(timerResolution, WheelSize, WaitStrategies.yieldingWait()).start()

        def expectedDelayNanos = TimeUnit.NANOSECONDS.convert(Delay, DelayUnits)
        List<TestResult> results = (1..Runs).collect({ run ->
            sleep 10
            def start = System.nanoTime()
            def executedTime = timer.schedule({ -> System.nanoTime() }, Delay, DelayUnits).get()
            def actual = executedTime - start
            return new TestResult(actual, actual - expectedDelayNanos)
        })
        def badRuns = results.count { result -> DelayUnits.convert(result.actual, TimeUnit.NANOSECONDS) + 1 < Delay - DelayUnits.convert(Resolution, ResolutionUnits) }

        expect: "the error should be within 15 percent (i.e. 1.5 ms)"
        println "requested delay: ${delay}; resolution: ${resolution}; bad runs: ${badRuns}"
        badRuns == 0

        and:
        timer.shutdown()

        where:
        Resolution | ResolutionUnits       | Delay | DelayUnits            | WheelSize | Runs
        100        | TimeUnit.MICROSECONDS | 200   | TimeUnit.MICROSECONDS | 512       | 10
        200        | TimeUnit.MICROSECONDS | 1     | TimeUnit.MILLISECONDS | 512       | 10
        1          | TimeUnit.MILLISECONDS | 10    | TimeUnit.MILLISECONDS | 512       | 10

        resolution = "${Resolution} ${units(ResolutionUnits)}"
        delay = "${Delay} ${units(DelayUnits)}"
    }

    class TestResult {
        long actual
        long error

        TestResult(actual, error) {
            this.actual = actual
            this.error = error
        }
    }

    def "should be able to schedule a one-shot task and then shutdown the timer immediately"() {
        setup: "create, start, and prime the timer"
        def timerResolution = Duration.ofSeconds(1)
        def timer = timer(timerResolution, 512, WaitStrategies.yieldingWait()).start()

        def delay = 100
        def units = TimeUnit.SECONDS

        when: "we schedule a task with a 10 ms delay"
        timer.schedule({ -> System.nanoTime() }, delay, units)

        and: "we sleep for 500 ms"
        sleep 500

        then: "the time should be running"
        !timer.isShutdown()
        !timer.isTerminated()

        when: "we shutdown immediately, then the scheduled job is cancelled"
        timer.shutdownNow()

        then:
        timer.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
    }

    def "should be able to schedule a one-shot task and then have the timer-shutdown wait until the task is done"() {
        setup: "create, start, and prime the timer"
        def timerResolution = Duration.ofMillis(100)
        def timer = timer(timerResolution, 512, WaitStrategies.yieldingWait()).start()

        def delay = 1000
        def units = TimeUnit.MILLISECONDS

        when: "we schedule a task with a 10 ms delay"
        timer.schedule({ -> System.nanoTime() }, delay, units)

        then: "the time should be running"
        !timer.isShutdown()
        !timer.isTerminated()

        when: "we shutdown immediately, then the scheduled job is cancelled"
        def start = System.nanoTime()
        timer.shutdown()

        and:
        def isShutdown = timer.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
        def shutdownTime = System.nanoTime()

        then:
        isShutdown

        and:
        Math.max(0, shutdownTime - start - TimeUnit.NANOSECONDS.convert(delay, units)) <= timerResolution.toNanos()
    }

    def "should be able to schedule a one-shot task and then cancel it"() {
        setup: "create, start, and prime the timer"
        def timerResolution = Duration.ofSeconds(1)
        def timer = timer(timerResolution, 512, WaitStrategies.yieldingWait()).start()

        def delay = 100
        def units = TimeUnit.SECONDS

        when: "we schedule a task with a 10 ms delay"
        def taskFuture = timer.schedule({ -> System.nanoTime() }, delay, units)

        and: "we sleep for 500 ms"
        sleep 500

        then: "we should be able to cancel the task"
        def start = System.currentTimeMillis()
        taskFuture.cancel(true)

        and: "the task should be cancelled"
        taskFuture.isCancelled()

        and:
        System.currentTimeMillis() - start <= 1000

        cleanup: "we shutdown immediately, then the scheduled job is cancelled"
        timer.shutdown()
    }

    @Unroll
    "fixed delay timer with resolution #resolution"() {
        setup:
        def timerResolution = Duration.ofNanos(TimeUnit.NANOSECONDS.convert(Resolution, ResolutionUnits))
        def timer = timer(timerResolution, WheelSize, WaitStrategies.yieldingWait()).start()

        def executionTimes = new ArrayList(10_000) as List<Long>
        def start = new AtomicLong(System.nanoTime())
        timer.scheduleWithFixedDelay({ ->
            final long execTime = System.nanoTime()
            final long oldStart = start.getAndSet(execTime)
            executionTimes.add(execTime - oldStart)
            return executionTimes
        }, Timeout, TimeoutUnits, Delay, Delay, DelayUnits).get()

        executionTimes.eachWithIndex { t, index ->
            println(
                    String.format("%,4d) %,10d %s", index, DelayUnits.convert(t, TimeUnit.NANOSECONDS), units(DelayUnits))
            )
        }

        where:
        Resolution | ResolutionUnits       | Delay | DelayUnits            | WheelSize | Accuracy | Timeout | TimeoutUnits
        200        | TimeUnit.MICROSECONDS | 50    | TimeUnit.MILLISECONDS | 512       | 2.0      | 1       | TimeUnit.SECONDS
        200        | TimeUnit.MICROSECONDS | 200   | TimeUnit.MICROSECONDS | 512       | 2.0      | 10      | TimeUnit.MILLISECONDS

        resolution = "${Resolution} ${units(ResolutionUnits)}"
    }

    @Unroll
    "fixed rate timer with resolution #resolution"() {
        setup:
        def timerResolution = Duration.ofNanos(TimeUnit.NANOSECONDS.convert(Resolution, ResolutionUnits))
        def timer = timer(timerResolution, WheelSize, WaitStrategies.yieldingWait()).start()
//        def timer = timer(timerResolution, WheelSize, WaitStrategies.busySpinWait()).start()

        def executionTimes = new ArrayList(10_000) as List<Long>
        def start = new AtomicLong(System.nanoTime())
        timer.scheduleAtFixedRate({ ->
            final long execTime = System.nanoTime()
            final long oldStart = start.getAndSet(execTime)
            executionTimes.add(execTime - oldStart)
            return executionTimes
        }, Timeout, TimeoutUnits, Delay, Delay, DelayUnits).get()

        executionTimes.eachWithIndex { t, index ->
            println(
                    String.format("%,4d) %,10d %s", index, DelayUnits.convert(t, TimeUnit.NANOSECONDS), units(DelayUnits))
            )
        }

        and:
        timer.shutdown()

        where:
        Resolution | ResolutionUnits       | Delay | DelayUnits            | WheelSize | Accuracy | Timeout | TimeoutUnits
        200        | TimeUnit.MICROSECONDS | 1000   | TimeUnit.MICROSECONDS | 512       | 2.0      | 100       | TimeUnit.MILLISECONDS
        200        | TimeUnit.MICROSECONDS | 1000  | TimeUnit.MICROSECONDS | 512       | 2.0      | 1       | TimeUnit.SECONDS
        200        | TimeUnit.MICROSECONDS | 50    | TimeUnit.MILLISECONDS | 512       | 2.0      | 1       | TimeUnit.SECONDS
        200        | TimeUnit.MICROSECONDS | 1000  | TimeUnit.MICROSECONDS | 512       | 2.0      | 100     | TimeUnit.MILLISECONDS

        resolution = "${Resolution} ${units(ResolutionUnits)}"
    }
}
