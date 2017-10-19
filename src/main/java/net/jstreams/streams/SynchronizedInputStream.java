package net.jstreams.streams;

import java.io.IOException;
import java.io.InputStream;


public class SynchronizedInputStream extends InputStream {

    private InputStream underlyingStream;

    public SynchronizedInputStream(InputStream underlyingStream) {
        this.underlyingStream = underlyingStream;
    }

    @Override
    public synchronized int read() throws IOException {
        return underlyingStream.read();
    }

    @Override
    public synchronized int read(byte[] b) throws IOException {
        return underlyingStream.read(b);
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        return underlyingStream.read(b, off, len);
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        return underlyingStream.skip(n);
    }

    @Override
    public synchronized int available() throws IOException {
        return underlyingStream.available();
    }

    @Override
    public synchronized void close() throws IOException {
        underlyingStream.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        underlyingStream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        underlyingStream.reset();
    }

    @Override
    public synchronized boolean markSupported() {
        return underlyingStream.markSupported();
    }
}
