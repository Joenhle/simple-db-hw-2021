package simpledb.util;

import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache extends LinkedHashMap<PageId, Page> {
    private int capacity;

    public LRUCache(int capacity) {
        super(capacity, 1, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<PageId, Page> eldest) {
        return size() > capacity;
    }
}

