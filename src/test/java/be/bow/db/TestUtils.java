package be.bow.db;

import java.util.Random;

public class TestUtils {

    private static Random random = new Random();

    public static String randomString(int length) {
        char[] result = new char[length];
        for (int i = 0; i < length; i++) {
            result[i] = (char) ('A' + random.nextInt('z' - 'A'));
        }
        String resultS = new String(result);
        return resultS;
    }

}
