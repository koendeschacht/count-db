package be.bagofwords.db;

public class ObjectType<T> {

    public final Class<T> _class;
    public final Class[] genericArguments;

    public ObjectType(Class<T> _class) {
        this(_class, new Class[0]);
    }

    public ObjectType(Class<T> _class, Class... genericArguments) {
        this._class = _class;
        this.genericArguments = genericArguments;
    }
}
