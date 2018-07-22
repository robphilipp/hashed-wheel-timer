package com.piggy.spiked.timing

import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration
import java.util.concurrent.TimeUnit

class HashedWheelTimerTest extends Specification {

    /**
     * Creates a hashed-wheel timer with specified resolution, wheel-size, and wait strategy
     * @param resolution The time-interval for each wheel bucket
     * @param wheelSize The number of buckets in the wheel
     * @param waitStrategy The strategy employed for waiting to advance the cursor
     * @return The timer
     */
    def oneShotTimer(Duration resolution, int wheelSize, WaitStrategy waitStrategy) {
        def timer = HashedWheelTimer.builder()
                .withDefaultTimerName()
                .withDefaultExecutor()
                .withResolution(resolution.toNanos(), TimeUnit.MICROSECONDS)
                .withWheelSize(wheelSize)
                .withWaitStrategy(waitStrategy)
                .build()

        return timer
    }

    def units(TimeUnit unit) {
        switch(unit) {
            case TimeUnit.NANOSECONDS: return "ns"
            case TimeUnit.MICROSECONDS: return "µs"
            case TimeUnit.MILLISECONDS: return "ms"
            case TimeUnit.SECONDS: return "s"
            default: return ""
        }
    }

    def "should be able to schedule a one-shot task"() {
        setup: "create, start, and prime the timer"
        def timer = oneShotTimer(Duration.ofMillis(1), 512, WaitStrategies.busySpinWait()).start()

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

        then: "the error should be within 15 percent (i.e. 1.5 ms)"
        def expectedDelayNanos = TimeUnit.NANOSECONDS.convert(delay, units)
        Math.abs(actualDelay - expectedDelayNanos) / expectedDelayNanos <= 0.15

        cleanup: "shutdown the timer"
        timer.shutdown()
    }

    @Unroll
    "one-shot task with delay of #delay and resolution #resolution should accurate within #accuracy"() {
        setup: "create, start, and prime the timer"
        def timerResolution = Duration.ofNanos(TimeUnit.NANOSECONDS.convert(Resolution, ResolutionUnits))
        def timer = oneShotTimer(timerResolution, WheelSize, WaitStrategies.busySpinWait()).start()

        def primeStart = System.nanoTime()
        println((timer.schedule({ -> System.nanoTime() }, Delay, DelayUnits).get() - primeStart) / 1e6 + " ms")
        primeStart = System.nanoTime()
        println((timer.schedule({ -> System.nanoTime() }, Delay, DelayUnits).get() - primeStart) / 1e6 + " ms")

        expect: "we schedule a task with a 10 ms delay"
        def start = System.nanoTime()
        def executedTime = timer.schedule({ -> System.nanoTime() }, Delay, DelayUnits).get()

        and: "and calculate the actual delay"
        def actualDelay = executedTime - start

        and: "the error should be within 15 percent (i.e. 1.5 ms)"
        def expectedDelayNanos = TimeUnit.NANOSECONDS.convert(Delay, DelayUnits)
        Math.abs(actualDelay - expectedDelayNanos) / expectedDelayNanos <= Accuracy

        and:
        timer.shutdown()

        where:
        Resolution | ResolutionUnits       | Delay | DelayUnits            | WheelSize | Accuracy
        100        | TimeUnit.MICROSECONDS | 100   | TimeUnit.MICROSECONDS | 512       | 15.00
        100        | TimeUnit.MICROSECONDS | 100   | TimeUnit.MICROSECONDS | 512       | 10.00
        100        | TimeUnit.MICROSECONDS | 100   | TimeUnit.MICROSECONDS | 512       | 5.00
        100        | TimeUnit.MICROSECONDS | 100   | TimeUnit.MICROSECONDS | 512       | 5.00
        100        | TimeUnit.MICROSECONDS | 100   | TimeUnit.MICROSECONDS | 512       | 5.00
        100        | TimeUnit.MICROSECONDS | 100   | TimeUnit.MICROSECONDS | 512       | 3.00
        100        | TimeUnit.MICROSECONDS | 100   | TimeUnit.MICROSECONDS | 512       | 3.00
        100        | TimeUnit.MICROSECONDS | 100   | TimeUnit.MICROSECONDS | 512       | 3.00
        100        | TimeUnit.MICROSECONDS | 100   | TimeUnit.MICROSECONDS | 512       | 3.00

        100        | TimeUnit.MICROSECONDS | 1     | TimeUnit.MILLISECONDS | 512       | 2.00
        100        | TimeUnit.MICROSECONDS | 1     | TimeUnit.MILLISECONDS | 512       | 0.50
        100        | TimeUnit.MICROSECONDS | 1     | TimeUnit.MILLISECONDS | 512       | 0.35
        100        | TimeUnit.MICROSECONDS | 1     | TimeUnit.MILLISECONDS | 512       | 0.35
        1          | TimeUnit.MILLISECONDS | 10    | TimeUnit.MILLISECONDS | 512       | 0.15

        resolution = "${Resolution} ${units(ResolutionUnits)}"
        delay = "${Delay} ${units(DelayUnits)}"
        accuracy = "${(Accuracy * 100 as Long)} %"
    }
}
