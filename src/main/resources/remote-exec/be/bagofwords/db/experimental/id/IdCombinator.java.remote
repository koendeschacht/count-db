package be.bagofwords.db.experimental.id;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.exec.RemoteClass;
import be.bagofwords.exec.RemoteObjectConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RemoteClass
public class IdCombinator<S extends IdObject> implements Combinator<List> {

    private final Combinator<S> combinator;

    public IdCombinator(Combinator<S> combinator) {
        this.combinator = combinator;
    }

    @Override
    public List<S> combine(List first, List second) {
        Map<String, S> values = new HashMap<>();
        List combined = new ArrayList(first);
        combined.addAll(second);
        for (Object object : combined) {
            S value = (S) object;
            String id = value.getId();
            S currentValue = values.get(id);
            S newValue;
            if (currentValue != null) {
                newValue = combinator.combine(currentValue, value);
            } else {
                newValue = value;
            }
            values.put(id, newValue);
        }
        return new ArrayList<>(values.values());
    }

    @Override
    public void addRemoteClasses(RemoteObjectConfig objectConfig) {
        combinator.addRemoteClasses(objectConfig);
    }
}
