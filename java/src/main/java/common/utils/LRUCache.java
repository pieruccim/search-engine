package common.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import config.ConfigLoader;


public class LRUCache<KeyType, ValueType> {

    private int capacity;
    private LinkedHashMap<KeyType,ValueType> map;

    private final static boolean debug = ConfigLoader.getPropertyBool("cache.class.debug");

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

    private Semaphore mutex = new Semaphore(1);

    private static final int THRESHOLD = 10;

    public void put(KeyType key, ValueType value) {
        if ( !this.map.containsKey(key) && this.map.size() >= this.capacity + THRESHOLD){

            if(mutex.tryAcquire()) {

                Iterator<Map.Entry<KeyType,ValueType>> it = this.map.entrySet().iterator();
                for (int i = 0; i < this.map.size() - this.capacity; i++) {
                    
                    Entry<KeyType, ValueType> entry = it.next();
                    if(this.onRemotionCallable != null)
                        this.onRemotionCallable.apply(entry.getValue());
                    it.remove();
                    if(debug) {
                        System.out.println("removed key '"+ entry.getKey().toString() + "' from cache in order to add '" + key.toString() +"'");
                        System.out.println("Was effectively removed? " + !this.map.containsKey(key));
                        this.map.entrySet().iterator().forEachRemaining((Entry<KeyType, ValueType> f) -> {System.out.print(f.getKey().toString() + " ");});
                        System.out.println();
                    }

                }
                // should close the iterator?
                it = null;
                mutex.release();
            }

        }
        this.map.put(key, value);
    }

    public Collection<ValueType> values(){
        return this.map.values();
    }
}