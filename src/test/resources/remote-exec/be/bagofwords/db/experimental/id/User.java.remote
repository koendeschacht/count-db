package be.bagofwords.db.experimental.id;

import be.bagofwords.exec.RemoteClass;

@RemoteClass
public class User implements IdObject {

    public String id;
    public String firstName;
    public String lastName;

    public User(String id, String firstName, String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public User() {
    }

    @Override
    public String getId() {
        return id;
    }
}
