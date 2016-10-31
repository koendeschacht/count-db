package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.ui.UI;
import be.bagofwords.util.StringUtils;

/**
 * Created by koen on 31.10.16.
 */
public abstract class BaseDataInterface<T> {
    protected final Combinator<T> combinator;
    protected final Class<T> objectClass;
    protected final String name;
    protected final boolean isTemporaryDataInterface;
    private final Object closeLock = new Object();
    private boolean wasClosed;
    private boolean closeWasRequested;

    public BaseDataInterface(Class<T> objectClass, String name, boolean isTemporaryDataInterface, Combinator<T> combinator) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Name can not be null or empty");
        }
        this.objectClass = objectClass;
        this.name = name;
        this.isTemporaryDataInterface = isTemporaryDataInterface;
        this.combinator = combinator;
    }

    public abstract void dropAllData();

    public abstract void flush();

    public Combinator<T> getCombinator() {
        return combinator;
    }

    public abstract DataInterface getCoreDataInterface();

    public Class<T> getObjectClass() {
        return objectClass;
    }

    public String getName() {
        return name;
    }

    public final void close() {
        requestClose();
        ifNotClosed(() -> {
                    if (isTemporaryDataInterface) {
                        dropAllData();
                    }
                    flush();
                    doClose();
                    wasClosed = true;
                }
        );
    }

    protected void requestClose() {
        closeWasRequested = true;
    }

    protected abstract void doClose();

    public final boolean wasClosed() {
        return wasClosed;
    }

    protected final boolean closeWasRequested() {
        return closeWasRequested;
    }

    @Override
    protected void finalize() throws Throwable {
        ifNotClosed(() -> {
            if (!isTemporaryDataInterface()) {
                //the user did not close the data interface himself?
                UI.write("Closing data interface " + getName() + " because it is about to be garbage collected.");
            }
            close();
        });
        super.finalize();
    }

    public void ifNotClosed(ActionIfNotClosed action) {
        synchronized (closeLock) {
            if (!wasClosed()) {
                action.doAction();
            }
        }
    }

    public boolean isTemporaryDataInterface() {
        return isTemporaryDataInterface;
    }

    public interface ActionIfNotClosed {
        public void doAction();
    }
}
