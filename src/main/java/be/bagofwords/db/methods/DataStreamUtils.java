package be.bagofwords.db.methods;

public class DataStreamUtils {

    public static <T> int writeValue(T value, DataStream ds, ObjectSerializer<T> objectSerializer) {
        int objectSize = objectSerializer.getObjectSize();
        int startOfObj = ds.position;
        if (objectSize == -1) {
            //Keep a space to write the length of the object
            ds.skip(4);
        }
        objectSerializer.writeValue(value, ds);
        if (objectSize == -1) {
            int actualSizeOfObject = ds.position - startOfObj - 4;
            ds.writeInt(actualSizeOfObject, startOfObj);
            objectSize = actualSizeOfObject + 4; //Need space for size also
        }
        return objectSize;
    }

    public static <T> int getObjectSize(DataStream ds, ObjectSerializer<T> objectSerializer) {
        int objectSize = objectSerializer.getObjectSize();
        if (objectSize == -1) {
            objectSize = ds.readInt();
        }
        return objectSize;
    }
}
