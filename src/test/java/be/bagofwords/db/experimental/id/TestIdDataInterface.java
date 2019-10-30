package be.bagofwords.db.experimental.id;

import be.bagofwords.db.BaseTestDataInterface;
import be.bagofwords.db.DatabaseBackendType;
import be.bagofwords.db.DatabaseCachingType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class TestIdDataInterface extends BaseTestDataInterface {

    public TestIdDataInterface(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        super(type, backendType);
    }

    @Test
    public void testWriteRead() {
        IdDataInterface<User> userDI = dataInterfaceFactory.createIdDataInterface("user", User.class);
        String id = "my-id";
        User user = new User(id, "Koen", "Deschacht");
        userDI.write(user);
        userDI.flush();
        User readUser = userDI.read(id);
        assertEquals(id, readUser.id);
        assertEquals("Koen", readUser.firstName);
        assertEquals("Deschacht", readUser.lastName);
        userDI.dropAllData();
        userDI.close();
    }

    @Test
    public void testCustomCombinator() {
        IdDataInterface<User> userDI = dataInterfaceFactory.createIdDataInterface("user", User.class, new UserFieldCombinator());
        String id = "my-id";
        User user = new User(id, "Koen", null);
        userDI.write(user);
        userDI.flush();
        User readUser = userDI.read(id);
        assertEquals(id, readUser.id);
        assertEquals("Koen", readUser.firstName);
        assertNull(readUser.lastName);
        User user2 = new User(id, null, "Deschacht");
        userDI.write(user2);
        userDI.flush();
        readUser = userDI.read(id);
        assertEquals(id, readUser.id);
        assertEquals("Koen", readUser.firstName);
        assertEquals("Deschacht", readUser.lastName);
        User user3 = new User(id, "Joske", null);
        userDI.write(user3);
        userDI.flush();
        readUser = userDI.read(id);
        assertEquals(id, readUser.id);
        assertEquals("Joske", readUser.firstName);
        assertEquals("Deschacht", readUser.lastName);
        userDI.dropAllData();
        userDI.close();
    }

}
