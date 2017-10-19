package net.jstreams.streams;


import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;

public class Crc32OutputStream extends OutputStream {
    private OutputStream underlyingStream;
    private CRC32 crc32;

    public Crc32OutputStream(OutputStream underlyingStream) {
        this.underlyingStream = underlyingStream;
        this.crc32 = new CRC32();
    }

    @Override
    public synchronized void write(int b) throws IOException {
        underlyingStream.write(b);
        crc32.update(b);
    }

    @Override
    public synchronized void write(byte[] b) throws IOException {
        underlyingStream.write(b);
        crc32.update(b);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        underlyingStream.write(b, off, len);
        crc32.update(b, off, len);
    }

    @Override
    public synchronized void flush() throws IOException {
        underlyingStream.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        underlyingStream.close();
    }

    public synchronized long getCrc32() {
        return crc32.getValue();
    }
}
