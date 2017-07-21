package be.bagofwords.db.data;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.exec.RemoteClass;
import be.bagofwords.util.KeyValue;

import java.util.ArrayList;
import java.util.List;

@RemoteClass
public class ListCombinator<T> implements Combinator<List<KeyValue<T>>> {

    private final Combinator<T> valueCombinator;

    public ListCombinator(Combinator<T> valueCombinator) {
        this.valueCombinator = valueCombinator;
    }

    @Override
    public List<KeyValue<T>> combine(List<KeyValue<T>> first, List<KeyValue<T>> second) {
        List<KeyValue<T>> result = new ArrayList<>(first.size());
        int ind1 = 0;
        int ind2 = 0;
        while (ind1 < first.size() && ind2 < second.size()) {
            KeyValue<T> kv1 = first.get(ind1);
            KeyValue<T> kv2 = second.get(ind2);
            if (kv1.getKey() < kv2.getKey()) {
                result.add(kv1);
                ind1++;
            } else if (kv1.getKey() == kv2.getKey()) {
                T combined;
                if (kv1.getValue() == null || kv2.getValue() == null) {
                    combined = kv2.getValue();
                } else {
                    combined = valueCombinator.combine(kv1.getValue(), kv2.getValue());
                }
                result.add(new KeyValue<>(kv1.getKey(), combined));
                ind1++;
                ind2++;
            } else {
                result.add(kv2);
                ind2++;
            }
        }
        while (ind1 < first.size()) {
            result.add(first.get(ind1));
            ind1++;
        }
        while (ind2 < first.size()) {
            result.add(second.get(ind2));
            ind2++;
        }
        return result;
    }
}
