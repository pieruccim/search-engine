package common.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;


public class LRUCache<KeyType, ValueType> {

    private int capacity;
    private LinkedHashMap<KeyType,ValueType> map;

    private Function<ValueType, Boolean> onRemotionCallable;

    /**
     * 
     * @param capacity the number of objects that the cache can fit at most
     * @param onRemotionCallable a callable that will be executed on item eviction from cache
     */
    public LRUCache(int capacity, Function<ValueType, Boolean> onRemotionCallable) {
        this.capacity = capacity;
        this.onRemotionCallable = onRemotionCallable;
        this.map = new LinkedHashMap<>(16, 0.75f, true);
    }
    /**
     * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     * @param key
     * @return
     */
    public ValueType get(KeyType key) {
        ValueType value = this.map.get(key);
        return value;
    }

    public void put(KeyType key, ValueType value) {
        if (
            !this.map.containsKey(key) &&
            this.map.size() == this.capacity
        ) {
            Iterator<Map.Entry<KeyType,ValueType>> it = this.map.entrySet().iterator();
            Entry<KeyType, ValueType> entry = it.next();
            if(this.onRemotionCallable != null)
                this.onRemotionCallable.apply(entry.getValue());
            it.remove();
        }
        this.map.put(key, value);
    }

    public Collection<ValueType> values(){
        return this.map.values();
    }
}