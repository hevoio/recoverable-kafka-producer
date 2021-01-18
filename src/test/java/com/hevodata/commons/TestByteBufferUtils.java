package com.hevodata.commons;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class TestByteBufferUtils {

    @Test
    public void testUTF8String() {
        String value = "abcd";
        ByteBuffer byteBuffer = ByteBuffer.allocate(value.length() + 4);
        ByteBufferUtils.putUTF8String(byteBuffer, value);
        byteBuffer.flip();
        Assert.assertEquals(ByteBufferUtils.getUTF8String(byteBuffer), value);
    }

    @Test
    public void testBytes() {
        byte[] value = "abcd".getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(value.length + 4);
        ByteBufferUtils.putBytes(byteBuffer, value);
        byteBuffer.flip();
        Assert.assertArrayEquals(ByteBufferUtils.getBytes(byteBuffer), value);
        byteBuffer = ByteBuffer.allocate(4);
        value = null;
        ByteBufferUtils.putBytes(byteBuffer, value);
        byteBuffer.flip();
        Assert.assertNull(ByteBufferUtils.getBytes(byteBuffer));
    }
}