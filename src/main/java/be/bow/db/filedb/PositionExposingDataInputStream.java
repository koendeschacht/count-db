package be.bow.db.filedb;

import java.io.DataInputStream;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/15/14.
 */
public class PositionExposingDataInputStream extends DataInputStream {

    private int length;

    public PositionExposingDataInputStream(byte[] buffer) {
        super(new PositionExposingByteArrayInputStream(buffer));
        this.length = buffer.length;
    }

    public int getPosition() {
        return ((PositionExposingByteArrayInputStream) in).getPosition();
    }

    public void setPosition(int newPosition) {
        ((PositionExposingByteArrayInputStream) in).setPosition(newPosition);
    }

    public boolean hasMoreData() {
        return getPosition() < length;
    }

    public byte getCurrentByte() {
        return ((PositionExposingByteArrayInputStream) in).getBuffer()[getPosition()];
    }
}
