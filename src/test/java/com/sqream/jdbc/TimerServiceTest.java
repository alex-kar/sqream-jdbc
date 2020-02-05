package com.sqream.jdbc;

import org.junit.Test;

import static org.junit.Assert.*;

public class TimerServiceTest {

    @Test
    public void timingTest() throws InterruptedException {
        long WORK_TIME = 1_000;
        long SLEEP_TIME = 1_000;

        TimerService.work();
        Thread.sleep(WORK_TIME);
        TimerService.sleep();
        Thread.sleep(SLEEP_TIME);
        TimerService.work();
        System.out.println(TimerService.getReport());
    }

    @Test
    public void switchTest() throws InterruptedException {
        long WORK_TIME = 10;
        long SLEEP_TIME = 10;

        for (int i = 0; i < 1_000; i++) {
            TimerService.work();
            Thread.sleep(WORK_TIME);
            TimerService.work();
            Thread.sleep(WORK_TIME);
            TimerService.work();
            Thread.sleep(WORK_TIME);
            TimerService.sleep();
            Thread.sleep(SLEEP_TIME);
            TimerService.sleep();
            Thread.sleep(SLEEP_TIME);
            TimerService.sleep();
            Thread.sleep(SLEEP_TIME);
            TimerService.work();
        }
        System.out.println(TimerService.getReport());
    }
}