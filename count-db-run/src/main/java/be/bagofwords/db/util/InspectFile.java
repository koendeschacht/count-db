package be.bagofwords.db.util;

import be.bagofwords.logging.Log;
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
public class InspectFile {

    public static void main(String[] args) throws IOException {
        File fileToInspect = new File(UI.read("Please specify the file to inspect"));
        int length = (int) fileToInspect.length();
        byte[] buffer = new byte[length];
        FileInputStream fis = new FileInputStream(fileToInspect);
        int bytesRead = fis.read(buffer);
        if (bytesRead != buffer.length) {
            throw new RuntimeException("Expected to read " + buffer.length + " but read " + bytesRead);
        }
        inspectBuffer(buffer);
    }

    private static void inspectBuffer(byte[] buffer) {
        List<ReadValue> values = new ArrayList<>();
        boolean finished = false;
        while (!finished) {
            int action;
            do {
                action = UI.readInt("Inspecting buffer of length " + buffer.length + " Read (1) long, (2) int, (3) string or (4) stop");
            } while (action < 1 || action > 4);
            if (action == 4) {
                finished = true;
            } else {
                int startPos = UI.readInt("start position?");
                int endPos = -1;
                Object value = null;
                if (action == 1) {
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

    private static void printValues(List<ReadValue> values) {
        for (ReadValue value : values) {
            Object valueToPrint = value.getValue();
            if (valueToPrint instanceof String) {
                String stringValueToPrint = (String) valueToPrint;
                stringValueToPrint = replaceNonAscii(stringValueToPrint);
                if (stringValueToPrint.length() > 100) {
                    stringValueToPrint = stringValueToPrint.substring(0, 50) + " ... " + stringValueToPrint.substring(stringValueToPrint.length() - 50, stringValueToPrint.length());
                }
                valueToPrint = stringValueToPrint;
            }
            Log.i(value.getStart() + "-->" + value.getEnd() + "=" + valueToPrint);
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
