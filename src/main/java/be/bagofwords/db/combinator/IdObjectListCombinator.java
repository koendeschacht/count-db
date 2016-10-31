package be.bagofwords.db.combinator;

import be.bagofwords.db.IdObject;
import be.bagofwords.db.IdObjectList;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by koen on 31.10.16.
 */
public class IdObjectListCombinator<S, T extends IdObject<S>> implements Combinator<IdObjectList<S, T>> {

    private final Combinator<T> baseCombinator;

    public IdObjectListCombinator(Combinator<T> baseCombinator) {
        this.baseCombinator = baseCombinator;
    }


    @Override
    public IdObjectList<S, T> combine(IdObjectList<S, T> first, IdObjectList<S, T> second) {
        if (first == null || first.isEmpty()) {
            return second;
        }
        if (second == null || second.isEmpty()) {
            return first;
        }
        if (first.size() + second.size() < 10) {
            //Short lists, no need to worry about an efficient implementation
            IdObjectList<S, T> smallest = first.size() < second.size() ? first : second;
            IdObjectList<S, T> largest = first.size() < second.size() ? second : first;
            IdObjectList<S, T> result = new IdObjectList<>(largest);
            for (T object : smallest) {
                boolean foundObject = false;
                for (int i = 0; i < result.size(); i++) {
                    T other = result.get(i);
                    if (object.getId().equals(other.getId())) {
                        T combined = baseCombinator.combine(object, other);
                        result.set(i, combined);
                        foundObject = true;
                        break;
                    }
                }
                if (!foundObject) {
                    result.add(object);
                }
            }
            return result;
        } else {
            //Large lists. Let's optimize a little bit more
            Map<S, T> objects = new HashMap<>();
            addAndMerge(first, objects);
            addAndMerge(second, objects);
            return new IdObjectList<>(objects.values());
        }
    }

    private void addAndMerge(IdObjectList<S, T> first, Map<S, T> objects) {
        for (T object : first) {
            S id = object.getId();
            T curr = objects.get(id);
            if (curr != null) {
                objects.put(id, baseCombinator.combine(curr, object));
            } else {
                objects.put(id, object);
            }
        }
    }
}
