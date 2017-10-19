package net.jstreams.streams;

import java.io.IOException;
import java.io.OutputStream;

public class SynchronizedOutputStream  extends OutputStream {

    private OutputStream underlyingStream;

    public SynchronizedOutputStream(OutputStream underlyingStream) {
        this.underlyingStream = underlyingStream;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        underlyingStream.write(b);
    }

    @Override
    public synchronized void write(byte[] b) throws IOException {
        underlyingStream.write(b);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        underlyingStream.write(b, off, len);
    }

    @Override
    public synchronized void flush() throws IOException {
        underlyingStream.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        underlyingStream.close();
    }
}
