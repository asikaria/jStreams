package net.jstreams.streams;


import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class TestCoalescingOutputStreamSingleThreaded {

    @Test
    public void singleThreadedWrites() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(16384);
        CoalescingOutputStream cos = new CoalescingOutputStream(bos);

        PrintStream out = new PrintStream(cos);
        for (int i = 1; i <= 10; i++) {
            out.println("This is line #" + i);
            out.format("This is the same line (%d), but using formatted output. %n", i);
            Thread.yield();
        }
        out.close();
        assertTrue("arrays dont match", Arrays.equals(bos.toByteArray(), getSampleText1()));
    }

    @Test
    public void singleThreadedWrites2() throws IOException {
        int numLines = 10000;
        ByteArrayOutputStream bos = new ByteArrayOutputStream(8 * 1024 * 1024);
        CoalescingOutputStream cos = new CoalescingOutputStream(bos);

        PrintStream out = new PrintStream(cos, false, "UTF-8");
        for (int i = 0; i < numLines; i++) {
            out.format("This is line %d of the file.%n", i);
            Thread.yield();
        }
        out.close();

        InputValidator.validateFile(bos.toByteArray(), numLines);
    }


    @Test
    public void writeZeroBytes() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(16384);
        CoalescingOutputStream cos = new CoalescingOutputStream(bos);

        cos.close();
        assertTrue("arrays dont match", Arrays.equals(bos.toByteArray(), new byte[0]));
    }

    @Test
    public void writeOneByte() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(16384);
        CoalescingOutputStream cos = new CoalescingOutputStream(bos);

        cos.write(100);
        cos.close();

        byte[] oneByte = new byte[1];
        oneByte[0] = 100;

        assertTrue("arrays dont match", Arrays.equals(bos.toByteArray(), oneByte));
    }

    @Test
    public void write4MB() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(16384);
        CoalescingOutputStream cos = new CoalescingOutputStream(bos);

        byte[] contents = getRandomBuffer(4 * 1024 * 1024); // 4MB
        cos.write(contents);
        cos.close();

        assertTrue("arrays dont match", Arrays.equals(bos.toByteArray(), contents));
    }

    @Test
    public void write5MB() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(16384);
        CoalescingOutputStream cos = new CoalescingOutputStream(bos);

        byte[] contents = getRandomBuffer(5 * 1024 * 1024); // 4MB
        cos.write(contents);
        cos.close();

        assertTrue("arrays dont match", Arrays.equals(bos.toByteArray(), contents));
    }

    @Test
    public void write15MB() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(16384);
        CoalescingOutputStream cos = new CoalescingOutputStream(bos);

        byte[] contents = getRandomBuffer(15 * 1024 * 1024); // 4MB
        cos.write(contents);
        cos.close();

        assertTrue("arrays dont match", Arrays.equals(bos.toByteArray(), contents));
    }

    private static byte[] getSampleText1() {
        ByteArrayOutputStream b = new ByteArrayOutputStream(1024);
        PrintStream out = new PrintStream(b);
        try {
            for (int i = 1; i <= 10; i++) {
                out.println("This is line #" + i);
                out.format("This is the same line (%d), but using formatted output. %n", i);
            }
            out.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return b.toByteArray();
        // length of returned array is 742 bytes
    }

    public static byte[] getRandomBuffer(int len) {
        SecureRandom prng = new SecureRandom();
        byte[] b = new byte[len];
        prng.nextBytes(b);
        return b;
    }
}
