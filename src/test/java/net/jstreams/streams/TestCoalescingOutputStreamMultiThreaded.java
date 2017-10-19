/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 *
 */

package net.jstreams.streams;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCoalescingOutputStreamMultiThreaded {


    @Test
    public void testDifferentThreadCounts() throws Exception {
        testBattery(1);
        testBattery(6);
        testBattery(12);
        testBattery(60);
    }

    public void calibrationTest(int delay, int num1000s) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1 * 1024 * 1024 * 1024);  // 1 GB
        DelayOutputStream dos = new DelayOutputStream(bos, 0, delay, delay);
        OutputStream stream = dos;

        long start = System.currentTimeMillis();
        for (int i = 0; i < num1000s * 1000; i++) {
            try {
                byte[] line = String.format("This is line %d of the file.%n", i).getBytes("UTF-8");
                stream.write(line);
            } catch (Exception ex) {
                ex.printStackTrace();
                throw ex;
            }
        }
        stream.close();
        long end = System.currentTimeMillis();
        double duration = (end - start)/1000.0;
        byte[] fileContents = bos.toByteArray();

        System.out.format("%s\t%4d\t%9.3f seconds\t%d%n", "SingleThreadedBaseline", delay, duration, fileContents.length);
        InputValidator.validateFile(fileContents, num1000s * 1000);

    }


    public void testBattery(int numThreads)  throws Exception {
        int dataSize = 320;
        runDelayedCoalescingTest(numThreads, 0,    dataSize);
        runDelayedCoalescingTest(numThreads, 10,   dataSize);
        runDelayedCoalescingTest(numThreads, 100,  dataSize);
        runDelayedCoalescingTest(numThreads, 200,  dataSize);
        runDelayedCoalescingTest(numThreads, 400,  dataSize);
        runDelayedCoalescingTest(numThreads, 600,  dataSize);
        runDelayedCoalescingTest(numThreads, 800,  dataSize);
        runDelayedCoalescingTest(numThreads, 1000, dataSize);
        runDelayedCoalescingTest(numThreads, 1100, dataSize);
        runDelayedCoalescingTest(numThreads, 1200, dataSize);
        runDelayedCoalescingTest(numThreads, 1300, dataSize);
        runDelayedCoalescingTest(numThreads, 1400, dataSize);
        runDelayedCoalescingTest(numThreads, 2800, dataSize);
        runDelayedCoalescingTest(numThreads, 5600, dataSize);
        System.out.println();
    }

    public void runDelayedCoalescingTest(int numThreads, int delay, int num1000s) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1 * 1024 * 1024 * 1024);  // 1 GB
        DelayOutputStream dos = new DelayOutputStream(bos, 0, delay, delay);
        CoalescingOutputStream cos = new CoalescingOutputStream(dos, false);
        OutputStream stream = cos;
        String testName = "DelayedCoalescingTest";

        RunState state = new RunState(delay, numThreads, stream, num1000s);
        double duration = runTest(state);
        stream.close();

        byte[] fileContents = bos.toByteArray();
        InputValidator.validateFile(fileContents, num1000s * 1000);
        if (state.savedException != null) {
            throw state.savedException;
        }

        System.out.format("%s\t%3d\t%4d\t%9.3f seconds\t%d%n", testName, numThreads, delay, duration, fileContents.length);
    }

    public double runTest(RunState state) throws Exception {
        final int NUM_THREADS = state.numThreads;

        // create threads
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(new WriteThreadStarter(state), "WriterThread"+i);
        }

        // start threads
        long start = System.currentTimeMillis();
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].start();
        }
        // wait for threads to finish
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].join();
        }
        long end = System.currentTimeMillis();
        double duration = (end - start)/1000.0;

        return duration;
    }
}

class RunState {
    public int delay;
    public int numThreads;
    public OutputStream stream;
    public int num1000s;

    public AtomicInteger allocator1000 = new AtomicInteger(0);
    public volatile Exception savedException = null;

    public RunState(int delay, int numThreads, OutputStream stream, int num1000s) {
        this.delay = delay;
        this.numThreads = numThreads;
        this.stream = stream;
        this.num1000s = num1000s;
    }
}

class WriteThreadStarter implements Runnable {
    RunState state;

    public WriteThreadStarter(RunState state) {
        this.state = state;
    }

    public void run() {
        int block;
        while ((block = state.allocator1000.getAndIncrement()) < state.num1000s) {
            block = block * 1000;
            //System.out.println("Started block " + block);
            for (int i = block; i < block + 1000; i++) {
                try {
                    byte[] line = String.format("This is line %d of the file.%n", i).getBytes("UTF-8");
                    state.stream.write(line);
                    //Thread.yield();
                    //System.out.write(line);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    state.savedException = ex;
                    break;
                }
            }
        }
    }
}




