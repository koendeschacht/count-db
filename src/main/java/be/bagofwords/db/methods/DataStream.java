package be.bagofwords.db.methods;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class DataStream {

    private static final String NULL_VALUE = "xxx_NULL";

    public byte[] buffer;
    public int position;

    public DataStream() {
        this(new byte[1024]);
    }

    public DataStream(byte[] buffer) {
        this(buffer, 0);
    }

    public DataStream(byte[] buffer, int position) {
        this.buffer = buffer;
        this.position = position;
    }

    public void reset() {
        position = 0;
    }

    private void ensureSize(int size) {
        if (position + size > buffer.length) {
            byte[] newBuffer = new byte[Math.max(buffer.length * 2, position + size)];
            System.arraycopy(buffer, 0, newBuffer, 0, position);
            buffer = newBuffer;
        }
    }

    public long readLong() {
        long result = ((long) buffer[position] << 56) +
                ((long) (buffer[position + 1] & 255) << 48) +
                ((long) (buffer[position + 2] & 255) << 40) +
                ((long) (buffer[position + 3] & 255) << 32) +
                ((long) (buffer[position + 4] & 255) << 24) +
                (long) ((buffer[position + 5] & 255) << 16) +
                (long) ((buffer[position + 6] & 255) << 8) +
                (long) (buffer[position + 7] & 255);
        position += 8;
        return result;
    }

    public void writeLong(long value) {
        ensureSize(8);
        buffer[position] = (byte) (value >>> 56);
        buffer[position + 1] = (byte) (value >>> 48);
        buffer[position + 2] = (byte) (value >>> 40);
        buffer[position + 3] = (byte) (value >>> 32);
        buffer[position + 4] = (byte) (value >>> 24);
        buffer[position + 5] = (byte) (value >>> 16);
        buffer[position + 6] = (byte) (value >>> 8);
        buffer[position + 7] = (byte) (value);
        position += 8;
    }

    public int readInt() {
        int result = (buffer[position] << 24) +
                ((buffer[position + 1] & 255) << 16) +
                ((buffer[position + 2] & 255) << 8) +
                (buffer[position + 3] & 255);
        position += 4;
        return result;
    }

    public void writeInt(int value) {
        ensureSize(4);
        buffer[position] = (byte) ((value >>> 24));
        buffer[position + 1] = (byte) ((value >>> 16));
        buffer[position + 2] = (byte) ((value >>> 8));
        buffer[position + 3] = (byte) ((value));
        position += 4;
    }

    public void writeInt(int value, int position) {
        buffer[position] = (byte) ((value >>> 24));
        buffer[position + 1] = (byte) ((value >>> 16));
        buffer[position + 2] = (byte) ((value >>> 8));
        buffer[position + 3] = (byte) ((value));
    }

    public int readInt(int position) {
        int result = (buffer[position] << 24) +
                ((buffer[position + 1] & 255) << 16) +
                ((buffer[position + 2] & 255) << 8) +
                (buffer[position + 3] & 255);
        return result;
    }

    public boolean readBoolean() {
        return readByte() == 1;
    }

    public void writeBoolean(boolean value) {
        writeByte((byte) (value ? 1 : 0));
    }

    public byte readByte() {
        return buffer[position++];
    }

    public void writeByte(byte value) {
        ensureSize(1);
        buffer[position++] = value;
    }

    public void writeBytes(byte[] bytes) {
        ensureSize(bytes.length);
        System.arraycopy(bytes, 0, buffer, position, bytes.length);
        position += bytes.length;
    }

    public byte[] readBytes(int size) {
        byte[] result = new byte[size];
        System.arraycopy(buffer, position, result, 0, size);
        position += size;
        return result;
    }

    public void writeString(String value) {
        writeString(value, true);
    }

    public void writeString(String value, boolean storeLength) {
        if (value == null) {
            value = NULL_VALUE;
        } else if (value.equals(NULL_VALUE)) {
            throw new RuntimeException("Sorry value " + NULL_VALUE + " is a reserved value");
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (storeLength) {
            writeInt(bytes.length);
        }
        writeBytes(bytes);
    }

    public String readString() {
        int length = readInt();
        return readString(length);
    }

    public String readString(int length) {
        byte[] bytes = readBytes(length);
        String value = new String(bytes, StandardCharsets.UTF_8);
        if (value.equals(NULL_VALUE)) {
            value = null;
        }
        return value;
    }

    public byte[] getNonEmptyBytes() {
        if (position == buffer.length) {
            return buffer;
        } else {
            return Arrays.copyOf(buffer, position);
        }
    }

    public void skip(int size) {
        ensureSize(size);
        position += size;
    }

    public void moveDataToFront(int start, int end) {
        if (start < 0) {
            throw new RuntimeException("Invalid value for start " + start);
        }
        if (end < start) {
            throw new RuntimeException("Invalid value for end " + end);
        }
        if (end > buffer.length) {
            throw new RuntimeException("Invalid value for end " + end + " (buffer size is " + buffer.length + ")");
        }
        for (int i = start; i < end; i++) {
            buffer[i - start] = buffer[i];
        }
        position = end - start;
    }
}
