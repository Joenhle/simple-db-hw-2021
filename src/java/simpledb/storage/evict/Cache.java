package simpledb.storage.evict;

import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.Iterator;

public interface Cache {
    void remove(PageId pageId);
    void put(PageId pageId, Page page);
    Page get(PageId pageId);
    boolean containsKey(PageId pageId);
    Iterator<PageId> KeyIterator();
}
