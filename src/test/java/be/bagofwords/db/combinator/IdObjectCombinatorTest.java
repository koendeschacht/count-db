package be.bagofwords.db.combinator;

import be.bagofwords.db.IdObject;
import be.bagofwords.db.IdObjectList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by koen on 30.10.16.
 */
public class IdObjectCombinatorTest {

    private class Person implements IdObject<Integer> {
        public String name;
        public int id;
        public int websiteVisits;

        public Person(String name, int id, int websiteVisits) {
            this.name = name;
            this.id = id;
            this.websiteVisits = websiteVisits;
        }

        @Override
        public Integer getId() {
            return id;
        }
    }

    @Test
    public void combine() throws Exception {
        IdObjectList<Integer, Person> list1 = new IdObjectList<>(Arrays.asList(new Person("john", 1, 1), new Person("jane", 2, 5), new Person("kim", 4, 1)));
        IdObjectList<Integer, Person> list2 = new IdObjectList<>(Arrays.asList(new Person("mike", 5, 1), new Person("jane", 2, 1), new Person("timmy", 6, 1)));
        IdObjectList<Integer, Person> result = new IdObjectListCombinator<>(new PersonCombinator()).combine(list1, list2);
        Assert.assertNotNull(result);
        Assert.assertEquals(5, result.size());
        Person jane = null;
        for (Person person : result) {
            if (person.name.equals("jane")) {
                jane = person;
            }
        }
        Assert.assertNotNull(jane);
        Assert.assertEquals(2, jane.id);
        Assert.assertEquals(6, jane.websiteVisits);
    }

    private class PersonCombinator implements Combinator<Person> {

        @Override
        public Person combine(Person first, Person second) {
            if (!first.getId().equals(second.getId())) {
                throw new RuntimeException("Can not combine 2 persons with different ids!");
            }
            return new Person(first.name, first.id, first.websiteVisits + second.websiteVisits);
        }
    }
}