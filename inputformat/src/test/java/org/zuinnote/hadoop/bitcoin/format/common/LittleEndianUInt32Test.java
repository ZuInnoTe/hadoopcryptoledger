package org.zuinnote.hadoop.bitcoin.format.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LittleEndianUInt32Test {

    @Test
    public void testZero() {
        byte[] zeroBytes = { 0x00, 0x00, 0x00, 0x00 };
        LittleEndianUInt32 zero = new LittleEndianUInt32(zeroBytes);
        assertEquals(0L, zero.getValue());
    }

    @Test
    public void testOne() {
        byte[] oneBytes = { 0x01, 0x00, 0x00, 0x00 };
        LittleEndianUInt32 one = new LittleEndianUInt32(oneBytes);
        assertEquals(1L, one.getValue());
    }

    @Test
    public void testMaxUInt() {
        int[] allOnes = { 0xff, 0xff, 0xff, 0xff };
        LittleEndianUInt32 maxUInt = new LittleEndianUInt32(asByte(allOnes));
        assertEquals(4294967295L, maxUInt.getValue());
    }

    @Test
    public void testBig() {
        long testValue = (long) Integer.MAX_VALUE + 5L;
        LittleEndianUInt32 big = new LittleEndianUInt32(testValue);
        assertEquals(big.getValue(), testValue);
    }

    public byte[] asByte(int[] bytes) {
        byte[] result = new byte[bytes.length];
        for(int i=0; i<bytes.length; i++) {
            result[i] = (byte) bytes[i];
        }
        return result;
    }
}
