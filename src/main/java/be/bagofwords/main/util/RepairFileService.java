package be.bagofwords.main.util;

import be.bagofwords.application.annotations.BowComponent;
import be.bagofwords.ui.UI;
import be.bagofwords.util.SerializationUtils;
import org.apache.commons.lang3.CharUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 01/10/14.
 */

@BowComponent
public class RepairFileService {

    private void repairFile(File file) throws IOException {
        byte[] buffer = readFile(file);
        List<ReadValue> values = new ArrayList<>();
        int position = 0 ;
        while (position<buffer.length) {
            long key = SerializationUtils.bytesToLong(buffer, position) ;
            position += 8 ;
            int length = SerializationUtils.bytesToInt(buffer, position) ;
            position += 4 ;
            int foundStartPos = -1 ;
            int tryStartPos = position ;

                    value = SerializationUtils.bytesToLong(buffer, startPos);
                    endPos = startPos + 8;
                } else if (action == 2) {
                    value = SerializationUtils.bytesToInt(buffer, startPos);
                    endPos = startPos + 4;
                } else if (action == 3) {
                    int length = UI.readInt("length of string?");
                    value = SerializationUtils.bytesToString(buffer, startPos, length);
                    endPos = startPos + length;
                }
                for (int i = 0; i < values.size(); i++) {
                    ReadValue curr = values.get(i);
                    if (curr.getStart() < endPos && startPos < curr.getEnd()) {
                        values.remove(i--);
                    }
                }
                values.add(new ReadValue(startPos, endPos, value));
                Collections.sort(values);
                printValues(values);
            }
        }
    }

    private byte[] readFile(File file) throws IOException {
        int length = (int) file.length();
        byte[] buffer = new byte[length];
        FileInputStream fis = new FileInputStream(file);
        int bytesRead = fis.read(buffer);
        if (bytesRead != buffer.length) {
            throw new RuntimeException("Expected to read " + buffer.length + " but read " + bytesRead);
        }
        return buffer;
    }

    private static void printValues(List<ReadValue> values) {
        for (ReadValue value : values) {
            Object valueToPrint = value.getValue();
            if (valueToPrint instanceof String) {
                valueToPrint = replaceNonAscii((String) valueToPrint);
            }
            UI.write(value.getStart() + "-->" + value.getEnd() + "=" + valueToPrint);
        }
    }

    private static String replaceNonAscii(String valueToPrint) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < valueToPrint.length(); i++) {
            if (CharUtils.isAscii(valueToPrint.charAt(i))) {
                result.append(valueToPrint.charAt(i));
            } else {
                result.append("?");
            }
        }
        return result.toString();
    }

    private static class ReadValue implements Comparable<ReadValue> {
        private int start;
        private int end;
        private Object value;

        private ReadValue(int start, int end, Object value) {
            this.start = start;
            this.end = end;
            this.value = value;
        }

        public int getStart() {
            return start;
        }

        public void setStart(int start) {
            this.start = start;
        }

        public int getEnd() {
            return end;
        }

        public void setEnd(int end) {
            this.end = end;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        @Override
        public int compareTo(ReadValue o) {
            return Integer.compare(getStart(), o.getStart());
        }
    }

}
