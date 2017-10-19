package net.jstreams.streams;


import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;

public class RandomMemoryInputStream extends InputStream {

    private static final int bufsize = 8 * 1024 * 1024; //64MB buffer
    private static final byte[] buffer = new byte[2*bufsize];

    private long streamLength = 0;
    private int cursor = 0;
    private boolean streamClosed = false;

    //static initializer, fill random numbers - shared by all instances
    static {
        byte[] buffertemp = new byte[bufsize];
        SecureRandom prng = new SecureRandom();
        prng.nextBytes(buffertemp);
        System.arraycopy(buffertemp, 0, buffer, 0, buffertemp.length);
        System.arraycopy(buffertemp, 0, buffer, buffertemp.length, buffertemp.length); // second copy
    }


    public RandomMemoryInputStream() { this(Long.MAX_VALUE); }
    public RandomMemoryInputStream(long streamLength) {
        this.streamLength = streamLength;
    }


    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int i = read(b, 0, 1);
        if (i<0) return i;
        else return (b[0] & 0xFF);
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (b == null) {
            throw new NullPointerException("null byte array passed in to read() method");
        }
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (streamClosed) throw new IOException("attempting to read from a closed stream");
        if (b == null) {
            throw new NullPointerException("null byte array passed in to read() method");
        }

        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }

        if (len == 0) {
            return 0;
        }

        // EOF
        if (cursor >= streamLength) return -1;

        // can only return max of buffersize in one read
        if (len>bufsize) len = bufsize;

        // limit read to length of stream
        if (cursor + len > streamLength) len = (int) streamLength - cursor;

        int readOffset = cursor % bufsize;
        System.arraycopy(buffer, readOffset, b, off, len);
        cursor += len;

        return len;
    }

    public void seek(long n) throws IOException, EOFException {

        if (streamClosed) throw new IOException("attempting to seek into a closed stream;");
        if (n<0) throw new EOFException("Cannot seek to before the beginning of file");
        if (n>streamLength) throw new EOFException("Cannot seek past end of file");
        if (n > Integer.MAX_VALUE) throw new IllegalArgumentException("offset too large");

        cursor = (int) n;
    }

    public long getPos() throws IOException {
        if (streamClosed) throw new IOException("attempting to call getPos() on a closed stream");
        return cursor;
    }

    @Override
    public long skip(long n) throws IOException {
        if (streamClosed) throw new IOException("attempting to skip() on a closed stream");
        long newPos = cursor + n;
        if (newPos < 0) {
            newPos = 0;
            n = newPos - cursor;
        }
        if (newPos > streamLength) {
            newPos = streamLength;
            n = newPos - cursor;
        }
        seek(newPos);
        return n;
    }

    @Override
    public int available() throws IOException {
        return (int) Math.min(streamLength - cursor, (long)Integer.MAX_VALUE);
    }

    @Override
    public void close() throws IOException {
        streamClosed = true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
    }

    @Override
    public boolean markSupported() {
        return false;
    }
}
