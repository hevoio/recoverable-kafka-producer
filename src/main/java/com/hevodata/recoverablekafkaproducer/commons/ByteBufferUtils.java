package com.hevodata.recoverablekafkaproducer.commons;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteBufferUtils {

    public static final int SIZE_OF_INT = 4;

    public static void putUTF8String(ByteBuffer byteBuffer, String value) {
        byte[] valueArray = value.getBytes(StandardCharsets.UTF_8);
        byteBuffer.putInt(valueArray.length);
        byteBuffer.put(valueArray);
    }

    public static String getUTF8String(ByteBuffer byteBuffer) {
        int length = byteBuffer.getInt();
        byte[] dstArray = new byte[length];
        byteBuffer.get(dstArray);
        return new String(dstArray, StandardCharsets.UTF_8);
    }

    public static void putBytes(ByteBuffer byteBuffer, byte[] bytes) {
        if (bytes != null) {
            byteBuffer.putInt(bytes.length);
            byteBuffer.put(bytes);
            return;
        }
        //put just the length
        byteBuffer.putInt(0);
    }


    public static byte[] getBytes(ByteBuffer byteBuffer) {
        int length = byteBuffer.getInt();
        if (length == 0) {
            return null;
        }
        byte[] dstArray = new byte[length];
        byteBuffer.get(dstArray);
        return dstArray;
    }


    // Returns total number of bytes read.
    // In most cases, will be equal to `len` unless actual bytes present is less than `len`.
    public static int forceRead(InputStream inputStream, byte[] buffer, int len) throws IOException  {
        int totalBytesRead = 0;
        int offset = 0;
        int toRead = len;

        while (totalBytesRead != len) {
            int bytesRead = inputStream.read(buffer, offset, toRead);
            if (bytesRead == -1) {
                return totalBytesRead;
            }
            totalBytesRead = totalBytesRead + bytesRead;
            offset = totalBytesRead;
            toRead = toRead - totalBytesRead;
        }

        return totalBytesRead;
    }
}
