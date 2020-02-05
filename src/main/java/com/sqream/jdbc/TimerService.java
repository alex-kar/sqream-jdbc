package com.sqream.jdbc;

import java.text.MessageFormat;

public class TimerService {
    private static long totalWork = 0;
    private static long totalSleep = 0;
    private static long timer = 0;
    private static boolean isWorking = false;
    private static long start = 0;
    private static long curTime;

    public static void work() {
        if (!isWorking) {
            isWorking = true;
            curTime = System.currentTimeMillis();
            if (timer != 0) {
                totalSleep += curTime - timer;
            } else {
                start = curTime;
            }
            timer = curTime;
        }
    }

    public static void sleep() {
        curTime = System.currentTimeMillis();
        if (isWorking) {
            isWorking = false;
            totalWork += curTime - timer;
            timer = curTime;
        }
    }

    public static String getReport() {
        long total = System.currentTimeMillis() - start;
        return MessageFormat.format(
                "Work time: [{0}], Sleep time: [{1}]. Total time: [{2}]", totalWork, totalSleep, total);
    }
}
