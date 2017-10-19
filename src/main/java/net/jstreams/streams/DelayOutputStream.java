package net.jstreams.streams;

import java.io.IOException;
import java.io.OutputStream;


public class DelayOutputStream extends OutputStream {

    OutputStream underlyingStream;
    int writeDelay, flushDelay, closeDelay; // in milliseconds


    public DelayOutputStream(OutputStream underlyingStream, int delay) {
        this(underlyingStream, delay, delay, delay);
    }

    public DelayOutputStream(OutputStream underlyingStream, int writeDelay, int flushDelay, int closeDelay) {
        this.underlyingStream = underlyingStream;
        this.writeDelay = writeDelay;
        this.flushDelay = flushDelay;
        this.closeDelay = closeDelay;
    }

    @Override
    public void write(int b) throws IOException {
        delay(writeDelay);
        if (underlyingStream != null) underlyingStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        delay(writeDelay);
        if (underlyingStream != null) underlyingStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delay(writeDelay);
        if (underlyingStream != null) underlyingStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        delay(flushDelay);
        if (underlyingStream != null) underlyingStream.flush();
    }

    @Override
    public void close() throws IOException {
        delay(closeDelay);
        if (underlyingStream != null) underlyingStream.close();
    }

    private void delay(int millis) {
        if (millis > 0) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
