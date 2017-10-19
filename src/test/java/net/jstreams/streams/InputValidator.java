/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 *
 */

package net.jstreams.streams;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InputValidator {
    int expectedLines;
    int[] countArray;

    private InputValidator(int n) {
        expectedLines = n;
        countArray = new int[expectedLines];
        // for (int i = 0; i < expectedLines; i++) { countArray[i] = 0; } // all initial values in java are guaranteed 0
    }

    private void add(int n) throws IOException {
        if (n >= expectedLines) throw new IOException("Line number unexpected " + n);
        if (n < 0) throw new IOException("Line number unexpected" + n);
        countArray[n]++;
    }

    public static void validateFile(byte[] b, int expectedLines) throws IOException {
        Pattern regex = Pattern.compile("This is line ([0-9]+) of the file.");
        InputValidator inputValidator = new InputValidator(expectedLines);
        try (
                ByteArrayInputStream bis = new ByteArrayInputStream(b);
                InputStreamReader isr = new InputStreamReader(bis, "UTF-8");
                BufferedReader br = new BufferedReader(isr);
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher m = regex.matcher(line);
                if (m.matches()) {
                    String lineNumberStr = m.group(1);
                    int lineNumber = Integer.parseInt(lineNumberStr);
                    inputValidator.add(lineNumber);
                } else {
                    throw new IOException(String.format("Line (length %d) doesnt match expected pattern: %s",
                            line.length(), StringLeft(line, 80) ));
                }
            }
        }
        if (!inputValidator.check()) throw new IOException("Validation check failed on file contents");
    }

    private boolean check() {
        boolean correct = true;
        for (int i = 0; i < expectedLines; i++) {
            if (countArray[i] != 1) {
                correct = false;
                System.out.println("Index " + i + " has count " + countArray[i]);
            }
        }
        return correct;
    }

    private static String StringLeft(String s, int n) {
        return s.substring(0, Math.min(n, s.length()));
    }
}