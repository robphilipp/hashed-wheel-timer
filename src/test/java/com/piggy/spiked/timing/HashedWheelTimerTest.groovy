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
                .build().start()

        return timer
    }

    def "should be able to set a variable"() {
        when:
        def three = 3
        def test = System.nanoTime()

        then:
        three == 3
    }

    def "should be able to schedule a one-shot task"() {
        when:
        def waitStrategy = new WaitStrategy.BusySpinWait()
        def timer = oneShotTimer(Duration.ofMillis(1), 512, waitStrategy)
//        timer.start()

        and:
        def executedTime = 0
        timer.schedule({ -> executedTime = System.nanoTime()}, 10, TimeUnit.MILLISECONDS)
        timer.schedule({ -> executedTime = System.nanoTime()}, 10, TimeUnit.MILLISECONDS)
        def start = System.nanoTime()
        timer.schedule({ -> executedTime = System.nanoTime()}, 10, TimeUnit.MILLISECONDS)

        and:
        sleep(15)

        and:
        def delay = (executedTime - start) / 1e6 as Long

        then:
        delay > 9

        and:
        delay < 11
//
//        cleanup:
//        if(timer != null) timer.shutdown()
    }
}
