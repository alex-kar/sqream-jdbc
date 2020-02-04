package com.sqream.jdbc;

import java.text.MessageFormat;

public class TimerService {
    private static long totalWork = 0;
    private static long totalSleep = 0;
    private static long timer = 0;
    private static boolean isWorking = false;

    public static void work() {
        if (!isWorking) {
            isWorking = true;
            if (timer != 0) {
                totalSleep += System.currentTimeMillis() - timer;
            }
            timer = System.currentTimeMillis();
        }
    }

    public static void sleep() {
        if (isWorking) {
            isWorking = false;
            totalWork += System.currentTimeMillis() - timer;
            timer = System.currentTimeMillis();
        }
    }

    public static String getReport() {
        return MessageFormat.format("Work time: [{0}], Sleep time: [{1}]", totalWork, totalSleep);
    }
}
