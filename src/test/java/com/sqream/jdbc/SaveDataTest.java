package com.sqream.jdbc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SaveDataTest {

    public static void main(String[] args) {
        int AMOUNT = 50;
        int SIZE = 100_000;

        checkByteBuffers(AMOUNT, SIZE);
        checkArrays(AMOUNT, SIZE);
    }

    private static void checkByteBuffers(int AMOUNT, int SIZE) {
        ByteBuffer[] byteBuffers = new ByteBuffer[AMOUNT];
        for (int i = 0; i < AMOUNT; i++) {
            byteBuffers[i] = ByteBuffer.allocateDirect(4 * SIZE).order(ByteOrder.LITTLE_ENDIAN);
        }

        long t0 = System.currentTimeMillis();
        for (int i = 0; i < AMOUNT; i++) {
            for (int j = 0; j < SIZE; j++) {
                byteBuffers[i].putInt(j);
            }
        }
        long t1 = System.currentTimeMillis();
        System.out.println("ButyBuffers: " + (t1 - t0));
    }

    private static void checkArrays(int AMOUNT, int SIZE) {
        Object[][] arrays = new Object[AMOUNT][SIZE];


        long t0 = System.currentTimeMillis();
        for (int i = 0; i < AMOUNT; i++) {
            for (int j = 0; j < SIZE; j++) {
                arrays[i][j] = j;
            }
        }
        long t1 = System.currentTimeMillis();
        System.out.println("Arrays: " + (t1 - t0));
    }
}
