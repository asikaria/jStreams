package net.jstreams.streams;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CoalescingOutputStream extends OutputStream {

    private OutputStream underlyingStream;
    private final int blocksize = 4 * 1024 * 1024;
    private final int BUFSIZE = blocksize;
    private byte[] buffer = new byte[BUFSIZE];
    private volatile boolean streamClosed = false;
    private AtomicInteger inProgressWrites = new AtomicInteger(0);
    private volatile IOException lastError = null;
    private volatile Thread shuttleThread = null;

    private volatile long writtenIndex;
    private volatile long flushedIndex;
    private volatile long persistedIndex;

    private class LockObj {}  // just so lock shows up as a class name in jcmd Thread.print output
    private LockObj lockObj = new LockObj();
    private ReentrantLock shuttleNotificationLock = new ReentrantLock(true);
    private Condition bufferWrite = shuttleNotificationLock.newCondition(); // notify when a write happens
    private Condition bufferFlush = shuttleNotificationLock.newCondition(); // notify when shuttle flushes from buffer


    class SingleReading {
        public long lockAcquisitionMillis = 0;
        public long writeSize = 0;
        SingleReading(long lockAcquisitionMillis, long writeSize) {
            this.lockAcquisitionMillis = lockAcquisitionMillis;
            this.writeSize = writeSize;
        }
    }

    class Instrumentation {
        List<SingleReading> list = new ArrayList<SingleReading>(256);

        void add(long lockAcquisitionMillis, long writeSize) {
            if (instrumented) {
                list.add(new SingleReading(lockAcquisitionMillis, writeSize));
            }
        }

        void dump() {
            if (instrumented) {
                for (SingleReading r : list) {
                    System.out.format("size: %d\tms: %d%n", r.writeSize, r.lockAcquisitionMillis);
                }
            }
        }
    }

    private boolean instrumented;
    Instrumentation statter = new Instrumentation();


    public CoalescingOutputStream(OutputStream underlyingStream) {
        this(underlyingStream, false);
    }

    public CoalescingOutputStream(OutputStream underlyingStream, boolean instrumented) {
        this.underlyingStream = underlyingStream;
        this.instrumented = instrumented;
    }


/*
There are customer writes that go into the buffer, and there is a "shuttle" in a separate background thread that
is constantly writing to the back-end whatever it found in the buffer. flush() has to ensure everything that is
currently in the buffer is written into backend before it succeeds.

Other notes:
1. Exceptions encountered by Shuttle are thrown by the next write or flush
2. Shuttle has a higher-priority lock. Shuttle can wait for any in-progress write (buffer-copy) to finish, but multiple
   writers should not starve the shuttle. This can have high impact on perf if lock priority is not ensured.
3. Big writes - these will be broken into smaller 4MB writes, and Shuttle should start writing the fragments of the
   write even as the write() is un-acknowledged - this is required to avoid deadlocking the write().
4. Write boundaries are preserved upto blocksize.


Write:
1. Block if Buffer doesn't have space for full write
2. Block if someone else is writing
3. Unblock Shuttle, if waiting

Shuttle Start:
1. Detect if shuttle needs to terminate (stream is closed, and there are no in-progress writes)
2. Block if there is nothing in buffer - i.e., wait for writers


Shuttle End:
1. Unblock writes
2. Unblock waiting flushes

Flush:
1. Read writtenIndex, and block until flushedIndex comes up to it

Close:
1. Shut down the shuttle, and prevent all future writes and flushes
2. But some writes may already be in progress. They go through, and shuttle closes only after the in-progress writes are done.

*/

    @Override
    public void write(int b) throws IOException {
        byte buf[] = new byte[1];
        buf[0] = (byte) b;
        write(buf, 0, 1);
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (b == null) return;
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }
        if (off > b.length || len > (b.length - off))
            throw new IllegalArgumentException("array offset and length are > array size");

        while (len > blocksize) {
            writeInternal(b, off, blocksize);
            off += blocksize;
            len -= blocksize;
        }
        writeInternal(b, off, len);
    }

    public void writeInternal(byte[] b, int off, int len) throws IOException {
        synchronized (lockObj) { // lockObj is used just by writers to synchronize among themselves
            shuttleNotificationLock.lock(); // this lock is used for coordinating between writers and the shuttle
            try {
                if (streamClosed) throw new IOException("attempting to write to a closed stream");
                if (shuttleThread == null)
                    shuttleThread = ShuttleStarter.startThread(this);  // one-time, when the first write comes in

                // available space = BUSIZE - occupied space;
                // len should be < available space, otherwise we wait for space to become available
                while (len > BUFSIZE - (writtenIndex - flushedIndex)) {  // while there isn't enough available space
                    bufferFlush.awaitUninterruptibly();
                }
                writeToBuffer(b, off, writtenIndex, len);
                writtenIndex += len;
                bufferWrite.signalAll();
            } finally {
                shuttleNotificationLock.unlock();
            }
        }
        if (lastError != null) throw lastError; // throw any exceptions that happened in Shuttle
    }

    private static class ShuttleStarter implements Runnable {
        private CoalescingOutputStream str;

        public ShuttleStarter(CoalescingOutputStream str) {
            this.str = str;
        }

        public void run() {
            str.shuttle();
        }

        public static Thread startThread(CoalescingOutputStream str) {
            ShuttleStarter shuttleStarter = new ShuttleStarter(str);
            Thread t = new Thread(shuttleStarter, "Shuttle");
            t.start();
            return t;
        }
    }

    private void shuttleBackup() {
        //shuttle is the only thread that writes flushedIndex
        long start, end, wait1 = 0, wait2 = 0;
        long length = 0, endpoint;

        shuttleLoop:
        while (true) {
            // we want the write/flush to the underlying stream outside of the lock
            endpoint = writtenIndex; // read writtenIndex and keep local snapshot
            if (endpoint > flushedIndex) {
                try {
                    length = (endpoint - flushedIndex);
                    underlyingStream.write(getFromBuffer(flushedIndex, (int) length));
                    underlyingStream.flush();
                } catch (IOException ex) {
                    lastError = ex;
                }
            }

            start = System.currentTimeMillis();
            shuttleNotificationLock.lock();
            try {
                end = System.currentTimeMillis();
                flushedIndex = endpoint;
                bufferFlush.signalAll();

                while ((writtenIndex == flushedIndex) && !streamClosed) {
                    bufferWrite.awaitUninterruptibly();
                }
                if ((writtenIndex == flushedIndex) && streamClosed) {
                    break shuttleLoop;
                }
                // now we have something in the buffer - go to next iteration of the loop
            } finally {
                shuttleNotificationLock.unlock();
            }
            wait2 = end - start;
            statter.add(wait1+wait2, length);
        }
        buffer = null; // release buffer
    }


    private void shuttle() {
        //shuttle is the only thread that writes flushedIndex and persistedIndex
        // write() consumes flushedIndex and flush() consumes persistedIndex
        long start, end, waittime;
        long length = 0, endpoint;
        byte[] contentToWrite = null;

        shuttleLoop:
        while (true) {

            start = System.currentTimeMillis();  // measure time taken to acquire lock
            shuttleNotificationLock.lock();
            try {
                end = System.currentTimeMillis();
                if (flushedIndex > 0) {
                    bufferFlush.signalAll();
                }

                while ((writtenIndex == flushedIndex) && !streamClosed) {
                    bufferWrite.awaitUninterruptibly();
                }
                if ((writtenIndex == flushedIndex) && streamClosed) {
                    break shuttleLoop;
                }

                // now we have something in the buffer
                endpoint = writtenIndex; // read writtenIndex and keep local snapshot
                length = (endpoint - flushedIndex);
                contentToWrite = getFromBuffer(flushedIndex, (int) length);
                flushedIndex = endpoint;
            } finally {
                shuttleNotificationLock.unlock();
            }
            waittime = end - start;
            statter.add(waittime, length);

            // we want the write/flush to the underlying stream outside of the lock
            try {
                underlyingStream.write(contentToWrite);
                underlyingStream.flush();
                persistedIndex = endpoint;
            } catch (IOException ex) {
                lastError = ex;
            }
        }
        buffer = null; // release buffer
    }


    byte[] getFromBuffer(long startpoint, int length) {
        byte[] returnBuffer = new byte[length];
        if (length == 0) return returnBuffer;
        long endpoint = startpoint + length;
        int physicalStartPoint = (int) startpoint % BUFSIZE;
        int physicalEndPoint = (int) endpoint % BUFSIZE;
        if (physicalStartPoint >= physicalEndPoint) { // == implies buffer wrapped over, we already checked length == 0
            System.arraycopy(buffer, physicalStartPoint, returnBuffer, 0, BUFSIZE - physicalStartPoint);
            System.arraycopy(buffer, 0, returnBuffer, BUFSIZE - physicalStartPoint, physicalEndPoint);
        } else {
            System.arraycopy(buffer, physicalStartPoint, returnBuffer, 0, length);
        }
        return returnBuffer;
    }

    void writeToBuffer(byte[] content, long contentOffset, long startpoint, int length) {
        if (length == 0) return;
        long endpoint = startpoint + length;
        int physicalStartPoint = (int) startpoint % BUFSIZE;
        int physicalEndPoint = (int) endpoint % BUFSIZE;
        if (physicalStartPoint >= physicalEndPoint) { // == implies buffer wrapped over, we already checked length == 0
            System.arraycopy(content, (int) contentOffset, buffer, physicalStartPoint, BUFSIZE - physicalStartPoint);
            System.arraycopy(content, (int) contentOffset + BUFSIZE - physicalStartPoint, buffer, 0, physicalEndPoint);
        } else {
            System.arraycopy(content, (int) contentOffset, buffer, physicalStartPoint, length);
        }
    }

    @Override
    public void flush() throws IOException {
        if (streamClosed) throw new IOException("attempting to flush a closed stream");
        flushInternal();
    }

    public void flushInternal() throws IOException {
        long indexToWaitFor = writtenIndex;
        shuttleNotificationLock.lock();
        try {
            while (persistedIndex < indexToWaitFor) {
                bufferFlush.awaitUninterruptibly();
            }
        } finally {
            shuttleNotificationLock.unlock();
        }
        if (lastError != null) throw lastError;  // throw any exceptions that happened in Shuttle
    }

    @Override
    public void close() throws IOException {
        if (streamClosed) return;
        flushInternal();

        // wake up the shuttle if it is awaiting, so it can terminate
        shuttleNotificationLock.lock();
        streamClosed = true;
        try {
            bufferWrite.signalAll();
        } finally {
            shuttleNotificationLock.unlock();
        }
        closeUnderlyingStream();
        statter.dump();
    }

    private void closeUnderlyingStream() throws IOException {
        try {
            if (shuttleThread != null) shuttleThread.join();
            underlyingStream.close();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();   // http://www.ibm.com/developerworks/library/j-jtp05236/
        }
    }
}
