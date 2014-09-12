package be.bow.db;

public interface ChangedValuesListener {

    public void valuesChanged(long[] keys);

}
