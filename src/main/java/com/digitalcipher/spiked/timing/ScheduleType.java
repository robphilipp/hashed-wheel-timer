package com.digitalcipher.spiked.timing;

/**
 * The type of schedule.
 * <ol>
 *     <li>ONE_SHOT - is a single execution after the specified delay</li>
 *     <li>FIXED_DELAY - executes the task periodically, with the same delay between the
 *          completion of the previous task execution and the start of the next one.</li>
 *     <li>FIXED_RATE - executes the task periodically, with the same delay between the
 *          start of the previous task execution and the start of the next one.</li>
 * </ol>
 */
public enum ScheduleType {
    ONE_SHOT,
    FIXED_DELAY,
    FIXED_RATE
}
