package com.piggy.spiked.timing

import spock.lang.Specification

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

    def "should be able to schedule a one-shot task"() {
        setup: "create, start, and prime the timer"
        def timer = oneShotTimer(Duration.ofMillis(1), 512, WaitStrategies.busySpinWait())
        timer.start()
        timer.schedule({ -> System.nanoTime()}, 10, TimeUnit.MILLISECONDS).get()

        when: "we schedule a task with a 10 ms delay"
        def start = System.nanoTime()
        def executedTime = timer.schedule({ -> System.nanoTime()}, 10, TimeUnit.MILLISECONDS).get()

        and: "and calculate the actual delay"
        def delay = (executedTime - start) / 1e3 as Long

        then: "the error should be within 1.5 percent (i.e. 1.5 ms)"
        Math.abs(delay - 10_000) <= 1500

        cleanup: "shutdown the timer"
        timer.shutdown()
    }
}
