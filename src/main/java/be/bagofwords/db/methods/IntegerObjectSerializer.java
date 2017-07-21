package be.bagofwords.db.methods;

/**
 * Created by koen on 23/05/17.
 */
public class IntegerObjectSerializer implements ObjectSerializer<Integer> {

    @Override
    public void writeValue(Integer obj, DataStream ds) {
        ds.writeInt(obj);
    }

    @Override
    public Integer readValue(DataStream ds, int size) {
        return ds.readInt();
    }

    @Override
    public int getObjectSize() {
        return 4;
    }
}
