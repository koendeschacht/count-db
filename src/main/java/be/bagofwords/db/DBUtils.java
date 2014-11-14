package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.util.KeyValue;

import java.util.Collections;
import java.util.List;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 31/10/14.
 */
public class DBUtils {

    public static final boolean DEBUG = false;

    public static <T> void mergeValues(List<KeyValue<T>> mergedValuesList, List<KeyValue<T>> unmergedValues, Combinator<T> combinator) {
        Collections.sort(unmergedValues);
        //combine values
        for (int i = 0; i < unmergedValues.size(); i++) {
            KeyValue<T> currPair = unmergedValues.get(i);
            long currKey = currPair.getKey();
            T currVal = currPair.getValue();
            for (int j = i + 1; j < unmergedValues.size(); j++) {
                long nextKey = unmergedValues.get(j).getKey();
                if (nextKey == currKey) {
                    T nextVal = unmergedValues.get(j).getValue();
                    T combinedVal;
                    if (currVal == null || nextVal == null) {
                        combinedVal = nextVal;
                    } else {
                        //Combine values
                        combinedVal = combinator.combine(currVal, nextVal);
                    }
                    currPair.setValue(combinedVal);
                    currVal = combinedVal;
                    i++;
                } else {
                    break;
                }
            }
            if (currPair.getValue() != null) {
                mergedValuesList.add(currPair);
            }
        }
    }
}
