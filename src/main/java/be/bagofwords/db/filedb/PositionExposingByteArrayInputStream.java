package be.bagofwords.db.filedb;

import java.io.ByteArrayInputStream;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/15/14.
 */
public class PositionExposingByteArrayInputStream extends ByteArrayInputStream {

    public PositionExposingByteArrayInputStream(byte[] buf) {
        super(buf);
    }

    public int getPosition() {
        return pos;
    }

    public void setPosition(int newPosition) {
        pos = newPosition;
    }

    public byte[] getBuffer() {
        return buf;
    }
}
