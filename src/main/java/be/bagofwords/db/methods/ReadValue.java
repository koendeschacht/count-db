package be.bagofwords.db.methods;

/**
 * Created by koen on 1/07/17.
 */
public class ReadValue<T> {
    public int size;
    public T value;

    public ReadValue(int size, T value) {
        this.size = size;
        this.value = value;
    }

}
