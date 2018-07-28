package com.piggy.spiked.timing

import spock.lang.Specification
import spock.lang.Unroll

class WaitStrategiesTest extends Specification {

    @Unroll
    "should create #expectedStrategy wait strategy from \"#Literal\""() {
        expect:
        WaitStrategies.from(Literal).orElse(null).class.getName() == ExpectedStrategy.getName()

        where:
        Literal                                         | ExpectedStrategy
        WaitStrategies.Strategy.BUSY_SPIN.literal()     | WaitStrategy.BusySpinWait.class
        WaitStrategies.Strategy.YIELDING_WAIT.literal() | WaitStrategy.YieldingWait.class
        WaitStrategies.Strategy.SLEEP_WAIT.literal()    | WaitStrategy.SleepWait.class

        expectedStrategy = ExpectedStrategy.getSimpleName()
    }
}
