package org.zuinnote.hadoop.bitcoin.format.common;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * An implementation of an unsigned 32-bit integer in Java.
 *
 * The value is stored in a raw little-endian four-byte buffer, and then converted
 * to a 64-bit signed long on-demand.  This allows UINT32 values  to be stored efficiently
 * for big-data computations while retaining the full range of positive values.
 */
public class LittleEndianUInt32 extends Number {

    public static final int NUM_BYTES = 4;
    public static final long MAX = (1L<<(NUM_BYTES*8)) - 1;

    /**
     * The raw data stored as 4 bytes in little-endian order.
     */
    protected ByteBuffer rawData;

    public LittleEndianUInt32() {
        this.rawData = ByteBuffer.allocate(NUM_BYTES);
        this.rawData.order(ByteOrder.LITTLE_ENDIAN);
    }

    public LittleEndianUInt32(byte[] value) {
        this();
        setValue(value);
    }

    public LittleEndianUInt32(long value) {
        this();
        setValue(value);
    }

    public LittleEndianUInt32(ByteBuffer value) {
        this();
        setValue(value);
    }

    public long getValue() {
        long value = rawData.getInt(0);
        if (value < 0) {
            return MAX+1+value;
        } else {
            return value;
        }
    }

    public void setValue(long value) {
        rawData.putInt(0, (int) value);
    }

    public void setValue(byte[] value) {
        rawData.put(value, 0, NUM_BYTES);
    }

    public void setValue(ByteBuffer buffer) {
        for(int i=0; i<NUM_BYTES; i++) {
           rawData.put(i, buffer.get());
        }
    }

    @Override
    public int intValue() {
        int result = (int) getValue();
        if (result < 0) throw new IllegalArgumentException("signed int overflow");
        return result;
    }

    @Override
    public long longValue() {
        return getValue();
    }

    @Override
    public float floatValue() {
        return (float) getValue();
    }

    @Override
    public double doubleValue() {
        return (double) getValue();
    }

}
