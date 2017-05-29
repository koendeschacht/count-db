package be.bagofwords.db;

import be.bagofwords.logging.Log;
import be.bagofwords.util.MappedLists;
import be.bagofwords.util.SerializationUtils;
import net.jpountz.lz4.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.xerial.snappy.SnappyFramedOutputStream.MAX_BLOCK_SIZE;

/**
 * Created by koen on 26/05/17.
 */
public class Tests {

    public static void main(String[] args) throws IOException {
        // testSnappy();
        testRandomAccess();
    }

    private static void testRandomAccess() throws IOException {
        Log.i("Generating file");
        String fileName = "/tmp/randomAccess.bin";
        createRandomFile(fileName);
        Log.i("Testing sequential access");
        MappedLists<String, Long> measuredTimes = new MappedLists<>();
        for (int run = 0; run < 10; run++) {
            measureReadTime("sequential", true, fileName, measuredTimes);
            measureReadTime("random", false, fileName, measuredTimes);
        }
        printTimes(measuredTimes);
    }

    private static void measureReadTime(String name, boolean sequential, String fileName, MappedLists<String, Long> measuredTimes) throws IOException {
        long start = System.nanoTime();
        File file = new File(fileName);
        if (sequential) {
            InputStream is = new BufferedInputStream(new FileInputStream(fileName));
            byte[] buffer = new byte[1024];
            long totalRead = 0;
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1 && totalRead < file.length() / 10) {
                totalRead += bytesRead;
            }
            is.close();
        } else {
            RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
            raf.seek(file.length() / 3 * 2);
            byte[] buffer = new byte[1024];
            raf.read(buffer);
        }
        measuredTimes.get(name).add((System.nanoTime() - start) / 1000_000);
    }

    private static void createRandomFile(String fileName) throws IOException {
        Random random = new Random();
        OutputStream os = new BufferedOutputStream(new FileOutputStream(fileName));
        byte[] buffer = new byte[1024];
        for (int i = 0; i < 1024 * 1024; i++) {
            random.nextBytes(buffer);
            os.write(buffer);
        }
        os.close();
    }

    public static void testSnappy() throws IOException {
        Random random = new Random();
        int numOfItems = 5_000_000;
        List<byte[]> randomItems = IntStream.range(0, numOfItems)
                .mapToLong(i -> random.nextLong())
                .mapToObj(SerializationUtils::longToBytes)
                .collect(toList());
        List<byte[]> consecItems = IntStream.range(0, numOfItems)
                .mapToLong(i -> i)
                .mapToObj(SerializationUtils::longToBytes)
                .collect(toList());
        List<byte[]> texts = readTexts(numOfItems);
        MappedLists<String, Long> measuredTimes = new MappedLists<>();
        for (int run = 0; run < 10; run++) {
            Log.i("Starting run " + run);
            measureTime("random", randomItems, "/tmp/", measuredTimes);
            measureTime("conseq", consecItems, "/tmp/", measuredTimes);
            measureTime("texts", texts, "/tmp/", measuredTimes);

            measureTime("random", randomItems, "/media/koen/LENOVO/tests/", measuredTimes);
            measureTime("conseq", consecItems, "/media/koen/LENOVO/tests/", measuredTimes);
            measureTime("texts", texts, "/media/koen/LENOVO/tests/", measuredTimes);
        }
        printTimes(measuredTimes);
    }

    private static void printTimes(MappedLists<String, Long> measuredTimes) {
        List<String> keys = measuredTimes.keySet().stream().sorted().collect(toList());
        for (String key : keys) {
            List<Long> values = measuredTimes.get(key);
            Log.i(key + " took median " + values.get(values.size() / 2));
        }
    }

    private static List<byte[]> readTexts(int numOfItems) throws IOException {
        List<byte[]> bytes = new ArrayList<>();
        BufferedReader rdr = new BufferedReader(new FileReader("/media/koen/LENOVO/koen_data/Downloads/enwiki-latest-pages-articles.xml"));
        while (rdr.ready() && bytes.size() < numOfItems / 10) {
            bytes.add(rdr.readLine().getBytes(StandardCharsets.UTF_8));
        }
        return bytes;
    }

    private static void measureTime(String name, List<byte[]> items, String directory, MappedLists<String, Long> measuredTimes) throws IOException {
        long start = System.nanoTime();
        OutputStream fos = new BufferedOutputStream(new FileOutputStream(directory + name + "out1.bin"));
        writeItems(items, fos);
        measuredTimes.get(name + " " + directory + " write file").add((System.nanoTime() - start) / 1000_000);
        start = System.nanoTime();
        InputStream fis = new BufferedInputStream(new FileInputStream(directory + name + "out1.bin"));
        readItems(fis);
        measuredTimes.get(name + " " + directory + " read file").add((System.nanoTime() - start) / 1000_000);
        start = System.nanoTime();
        final LZ4Compressor compressor = LZ4Factory.nativeInstance().fastCompressor();
        OutputStream sos = new LZ4BlockOutputStream(new FileOutputStream(directory + name + "out2.bin"), MAX_BLOCK_SIZE, compressor);
        writeItems(items, sos);
        measuredTimes.get(name + " " + directory + " write snappy").add((System.nanoTime() - start) / 1000_000);
        start = System.nanoTime();
        final LZ4FastDecompressor decompressor = LZ4Factory.nativeInstance().fastDecompressor();
        InputStream sis = new LZ4BlockInputStream(new FileInputStream(directory + name + "out2.bin"), decompressor);
        readItems(sis);
        measuredTimes.get(name + " " + directory + " read snappy").add((System.nanoTime() - start) / 1000_000);
    }

    private static void readItems(InputStream inputStream) throws IOException {
        List<Long> result = new ArrayList<>();
        byte[] buffer = new byte[8];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            result.add(SerializationUtils.bytesToLong(buffer));
        }
    }

    private static void writeItems(List<byte[]> items, OutputStream os) throws IOException {
        for (int i = 0; i < items.size(); i++) {
            os.write(items.get(i));
        }
        os.close();
    }

}
