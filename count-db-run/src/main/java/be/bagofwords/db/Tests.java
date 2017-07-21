package be.bagofwords.db;

import be.bagofwords.db.methods.DataStream;
import be.bagofwords.db.methods.JsonObjectSerializer;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.exec.RemoteObjectConfig;
import be.bagofwords.exec.RemoteObjectUtil;
import be.bagofwords.logging.Log;
import be.bagofwords.util.MappedLists;
import be.bagofwords.util.SerializationUtils;
import net.jpountz.lz4.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
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
        ObjectSerializer<TestObject> objectSerializer = new JsonObjectSerializer<>(TestObject.class, TestObject.class);
        RemoteObjectConfig execConfig = objectSerializer.createExecConfig();
        ObjectSerializer objectSerializer2 = (ObjectSerializer) RemoteObjectUtil.loadObject(execConfig.pack());
        Log.i("Success!");

        // testCompression();
        // testRandomAccess();
        // testRandomAccessWriting();
        // testBinarySearch();
        // testReadingStrings();
        testObjectSerializer();
    }

    private static void testObjectSerializer() throws IOException {
        Random random = new Random();
        int numOfItems = 40_000_000;
        List<Long> randomValues = new ArrayList<>();
        for (int i = 0; i < numOfItems; i++) {
            randomValues.add(random.nextLong());
        }
        boolean doBytes = false;
        boolean doNewStream = true;
        long start = System.currentTimeMillis();
        if (doBytes) {
            byte[] bytes = new byte[numOfItems * 8];
            int position = 0;
            for (int i = 0; i < numOfItems; i++) {
                SerializationUtils.longToBytes(randomValues.get(i), bytes, position);
                position += 8;
            }
            Log.i("Writing took " + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            position = 0;
            long sum = 0;
            for (int i = 0; i < numOfItems; i++) {
                sum += SerializationUtils.bytesToLong(bytes, position);
                position += 8;
            }
            Log.i("Reading took " + (System.currentTimeMillis() - start) + " sum is " + sum);
        } else if (doNewStream) {
            byte[] bytes = new byte[numOfItems * 8];
            DataStream dos = new DataStream(bytes);
            for (int i = 0; i < numOfItems; i++) {
                dos.writeLong(randomValues.get(i));
            }
            Log.i("Writing took " + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            dos.reset();
            long sum = 0;
            for (int i = 0; i < numOfItems; i++) {
                sum += dos.readLong();
            }
            Log.i("Reading took " + (System.currentTimeMillis() - start) + " " + sum);
        } else {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            for (int i = 0; i < numOfItems; i++) {
                dos.writeLong(randomValues.get(i));
            }
            dos.close();
            Log.i("Writing took " + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            byte[] bytes = bos.toByteArray();
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            DataInputStream dis = new DataInputStream(bis);
            for (int i = 0; i < numOfItems; i++) {
                dis.readLong();
            }
            dis.close();
            Log.i("Reading took " + (System.currentTimeMillis() - start));
        }
    }

    private static void testReadingStrings() throws IOException {
        List<String> strings = new ArrayList<>();
        List<byte[]> stringsAsBytes = new ArrayList<>();
        for (int i = 0; i < 4000000; i++) {
            String outS = "This is a some string This is a some string This is a some string This is a some string This is a some string This is a some string This is a some string This is a some string " + i;
            strings.add(outS);
            stringsAsBytes.add(outS.getBytes(StandardCharsets.UTF_8));
        }
        long start = System.currentTimeMillis();
        File outFile1 = new File("/tmp/test.bin");
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outFile1)));
        for (String string : strings) {
            byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
        dos.close();
        Log.i("Encoding took " + (System.currentTimeMillis() - start) + " length is " + outFile1.length());
        start = System.currentTimeMillis();
        File outFile2 = new File("/tmp/test2.bin");
        // ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outFile2)));
        // for (String string : strings) {
        //     be.bagofwords.db.test.Mystring mystring = new Mystring();
        //     mystring.setValue(string);
        //     oos.writeObject(mystring);
        // }
        // oos.close();
        OutputStream os = new BufferedOutputStream(new FileOutputStream(outFile2));
        byte[] lengthBytes = new byte[4];
        for (byte[] stringsAsByte : stringsAsBytes) {
            SerializationUtils.intToBytes(stringsAsByte.length, lengthBytes, 0);
            os.write(lengthBytes);
            os.write(stringsAsByte);
        }
        Log.i("Native took " + (System.currentTimeMillis() - start) + " length is " + outFile2.length());
    }

    public static void writeString(OutputStream dos, String s) throws IOException {
        for (int sIndex = 0, sLength = s.length(); sIndex < sLength; sIndex++) {
            char c = s.charAt(sIndex);
            if (c < '\u0080') {
                dos.write(c);
            } else if (c < '\u0800') {
                dos.write(192 | c >>> 6);
                dos.write(128 | c & 63);
            } else if (c < '\ud800' || c > '\udfff') {
                dos.write(224 | c >>> 12);
                dos.write(128 | c >>> 6 & 63);
                dos.write(128 | c & 63);
            } else {
                int cp = 0;
                if (++sIndex < sLength) cp = Character.toCodePoint(c, s.charAt(sIndex));
                if ((cp >= 1 << 16) && (cp < 1 << 21)) {
                    dos.write(240 | cp >>> 18);
                    dos.write(128 | cp >>> 12 & 63);
                    dos.write(128 | cp >>> 6 & 63);
                    dos.write(128 | cp & 63);
                } else
                    dos.write((byte) '?');
            }
        }
    }

    private static void testRandomAccessWriting() throws IOException {
        String fileName = "/tmp/randomAccessWriting.bin";
        Log.i("Testing random access writing");
        MappedLists<String, Long> measuredTimes = new MappedLists<>();
        for (int run = 0; run < 10; run++) {
            measureWriteTime("sequential", fileName, measuredTimes);
            measureWriteTime("random", fileName, measuredTimes);
        }
        printTimes(measuredTimes);
    }

    private static void measureWriteTime(String method, String fileName, MappedLists<String, Long> measuredTimes) throws IOException {
        Random random = new Random();
        int numOfItems = 1024 * 1024;
        long start = System.nanoTime();
        if (method.equals("sequential")) {
            DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
            for (int i = 0; i < numOfItems; i++) {
                os.writeLong(random.nextLong());
            }
            os.close();
        } else {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            for (int i = 0; i < numOfItems; i++) {
                dos.writeLong(random.nextLong());
            }
            dos.close();
            RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
            raf.write(bos.toByteArray());
            raf.close();
        }
        measuredTimes.get(method).add((System.nanoTime() - start) / 1000);
    }

    private static class Wrapper {
        public long value;

        public Wrapper(long value) {
            this.value = value;
        }
    }

    private static class Node {
        public long split;
        public Node left;
        public Node right;

        public Node(long split, Node left, Node right) {
            this.split = split;
            this.left = left;
            this.right = right;
        }

        public Node() {
            this.split = Long.MAX_VALUE;
        }

        public Node getNode(long value) {
            if (split == Long.MAX_VALUE) {
                return this;
            } else if (split > value) {
                return left.getNode(value);
            } else {
                return right.getNode(value);
            }
        }

        public int leafs() {
            if (split == Long.MAX_VALUE) {
                return 1;
            } else {
                return left.leafs() + right.leafs();
            }
        }
    }

    private static void testBinarySearch() {
        int numOfItems = 300_000;
        int numOfQueries = 100_000;
        List<Long> borders = new ArrayList<>();
        Random random = new Random(12);
        for (int i = 0; i < numOfItems; i++) {
            borders.add(random.nextLong());
        }
        Collections.sort(borders);
        Node tree = new Node();
        addNodes(tree, borders, 0, borders.size());
        List<Wrapper> borderWrappers = borders.stream().map(Wrapper::new).collect(toList());

        Log.i("Testing search methods ");
        MappedLists<String, Long> measuredTimes = new MappedLists<>();
        for (int run = 0; run < 20; run++) {
            measureBinarySearch("binarySearch", borders, tree, measuredTimes, numOfQueries, random, borderWrappers);
            measureBinarySearch("binaryWrapperSearch", borders, tree, measuredTimes, numOfQueries, random, borderWrappers);
            measureBinarySearch("tree", borders, tree, measuredTimes, numOfQueries, random, borderWrappers);
        }

        printTimes(measuredTimes);
    }

    private static void measureBinarySearch(String searchMethod, List<Long> borders, Node tree, MappedLists<String, Long> measuredTimes, int numOfQueries, Random random, List<Wrapper> borderWrappers) {
        long start = System.nanoTime();
        if (searchMethod.equals("binarySearch")) {
            for (int i = 0; i < numOfQueries; i++) {
                long value = random.nextLong();
                Collections.binarySearch(borders, value);
            }
        } else if (searchMethod.equals("binaryWrapperSearch")) {
            for (int i = 0; i < numOfQueries; i++) {
                long value = random.nextLong();
                Collections.binarySearch(borderWrappers, value, (o1, o2) -> Long.compare(((Wrapper) o1).value, (Long) o2));
            }
        } else {
            for (int i = 0; i < numOfQueries; i++) {
                long value = random.nextLong();
                tree.getNode(value);
            }
        }
        measuredTimes.get(searchMethod).add((System.nanoTime() - start) / 1000);
    }

    private static void addNodes(Node node, List<Long> borders, int start, int end) {
        if (end - start > 1) {
            int splitInd = start + (end - start) / 2;
            node.split = borders.get(splitInd);
            node.left = new Node();
            node.right = new Node();
            addNodes(node.left, borders, start, splitInd);
            addNodes(node.right, borders, splitInd, end);
        }
    }

    private static void testRandomAccess() throws IOException {
        Log.i("Generating file");
        String fileNameSSD = "/tmp/randomAccess.bin";
        String fileNameHD = "/media/koen/LENOVO/tests/randomAccess.bin";
        createRandomFile(fileNameSSD);
        createRandomFile(fileNameHD);
        Log.i("Testing sequential access");
        MappedLists<String, Long> measuredTimes = new MappedLists<>();
        for (int run = 0; run < 10; run++) {
            measureReadTime("sequential_ssd", fileNameSSD, measuredTimes);
            measureReadTime("random_seek_ssd", fileNameSSD, measuredTimes);
            measureReadTime("random_all_ssd", fileNameSSD, measuredTimes);
            measureReadTime("sequential_hd", fileNameHD, measuredTimes);
            measureReadTime("random_seek_hd", fileNameHD, measuredTimes);
            measureReadTime("random_all_hd", fileNameHD, measuredTimes);
        }
        printTimes(measuredTimes);
    }

    private static void measureReadTime(String name, String fileName, MappedLists<String, Long> measuredTimes) throws IOException {
        long start = System.nanoTime();
        Log.i("Started reading " + fileName);
        File file = new File(fileName);
        if (name.equals("sequential")) {
            InputStream is = new BufferedInputStream(new FileInputStream(fileName));
            byte[] buffer = new byte[1024 * 1024];
            long totalRead = 0;
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1 && totalRead < file.length() / 10) {
                totalRead += bytesRead;
            }
            is.close();
        } else if (name.equals("random_seek")) {
            RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
            raf.seek(file.length() / 3 * 2);
            byte[] buffer = new byte[1024];
            raf.read(buffer);
        } else if (name.equals("random_all")) {
            RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
            byte[] buffer = new byte[1024 * 1024];
            long totalRead = 0;
            int bytesRead;
            while ((bytesRead = raf.read(buffer)) != -1 && totalRead < file.length() / 10) {
                totalRead += bytesRead;
            }
            raf.close();
        }
        Log.i("Finished reading " + fileName);
        measuredTimes.get(name).add((System.nanoTime() - start));
    }

    private static void createRandomFile(String fileName) throws IOException {
        Random random = new Random();
        OutputStream os = new BufferedOutputStream(new FileOutputStream(fileName));
        byte[] buffer = new byte[1024];
        for (int i = 0; i < 1024 * 1024 * 10; i++) {
            random.nextBytes(buffer);
            os.write(buffer);
        }
        os.close();
    }

    public static void testCompression() throws IOException {
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
