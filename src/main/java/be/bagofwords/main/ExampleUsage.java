package be.bagofwords.main;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.application.EmbeddedDBContextFactory;
import be.bagofwords.db.combinator.OverWriteCombinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.KeyValue;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Date;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/23/14.
 */
public class ExampleUsage {

    public static void main(String[] args) throws ParseException {
        //create data interface factory that stores all data in /tmp/myData
        DataInterfaceFactory dataInterfaceFactory = EmbeddedDBContextFactory.createDataInterfaceFactory("/tmp/myData");

        //create data interfaces
        DataInterface<Long> myLogDataInterface = dataInterfaceFactory.createCountDataInterface("myLoginCounts");
        DataInterface<UserObject> myUserDataInterface = dataInterfaceFactory.createDataInterface("myUsers", UserObject.class, new OverWriteCombinator<>());

        //write data
        long userId = 12939;
        myLogDataInterface.increaseCount("user_" + userId + "_logged_in");
        myUserDataInterface.write(userId, new UserObject("koen", "deschacht", DateUtils.parseDate("1983-04-12", "yyyy-MM-dd")));

        //flush data (necessary to make the written data visible on next read)
        myLogDataInterface.flush();
        myUserDataInterface.flush();

        //read data
        long numOfLogins = myLogDataInterface.readCount("user_" + userId + "_logged_in");
        UserObject user = myUserDataInterface.read(userId);
        System.out.println("User " + user.getFirstName() + " " + user.getLastName() + " logged in " + numOfLogins + " times.");

        //iterate over all data
        CloseableIterator<KeyValue<UserObject>> iterator = myUserDataInterface.iterator();
        while (iterator.hasNext()) {
            KeyValue<UserObject> curr = iterator.next();
            UserObject currUser = curr.getValue();
            long currUserId = curr.getKey();
            System.out.println("User " + currUser.getFirstName() + " " + currUser.getLastName() + " with id " + currUserId);
        }
        iterator.close();

        //drop all data
        myLogDataInterface.dropAllData();
        myUserDataInterface.dropAllData();
    }

    public static class UserObject {

        private String firstName;
        private String lastName;
        private Date dateOfBirth;

        public UserObject(String firstName, String lastName, Date dateOfBirth) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.dateOfBirth = dateOfBirth;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public Date getDateOfBirth() {
            return dateOfBirth;
        }

        public void setDateOfBirth(Date dateOfBirth) {
            this.dateOfBirth = dateOfBirth;
        }

        //Constructor used for serialization
        public UserObject() {
        }
    }

}
