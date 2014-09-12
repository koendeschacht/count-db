package be.bow.db;

public interface ChangedValuesPublisher {

    public void registerListener(ChangedValuesListener listener);

}
