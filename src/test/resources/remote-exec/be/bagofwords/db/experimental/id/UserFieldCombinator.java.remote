package be.bagofwords.db.experimental.id;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.exec.RemoteClass;
import be.bagofwords.exec.RemoteObjectConfig;

import java.util.Objects;

import static org.apache.commons.lang3.StringUtils.isEmpty;

@RemoteClass
public class UserFieldCombinator implements Combinator<User> {

    @Override
    public User combine(User first, User second) {
        if (!Objects.equals(first.id, second.id)) {
            throw new RuntimeException("Will only combined objects with same id!");
        }
        User result = new User();
        result.id = first.id;
        result.firstName = isEmpty(second.firstName) ? first.firstName : second.firstName;
        result.lastName = isEmpty(second.lastName) ? first.lastName : second.lastName;
        return result;
    }

    @Override
    public void addRemoteClasses(RemoteObjectConfig objectConfig) {
        objectConfig.add(getClass());
        objectConfig.add(User.class);
    }
}
