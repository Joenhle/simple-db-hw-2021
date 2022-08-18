package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    class LRUCache {
        class Node {
            private PageId key;
            private Page page;
            private Node pre;
            private Node next;

            public Node(PageId key, Page page, Node pre, Node next) {
                this.key = key;
                this.page = page;
                this.pre = pre;
                this.next = next;
            }
        }
        private ConcurrentHashMap<PageId, Node> hashMap;
        private int capacity;
        private Node head, tail;
        public LRUCache(int capacity) {
            this.capacity = capacity;
            hashMap = new ConcurrentHashMap<>();
        }
        private void addLast(PageId pageId, Page page) {
            Node node = new Node(pageId, page, tail, null);
            if (tail != null) {
                tail.next = node;
            }
            if (head == null) {
                head = node;
            }
            tail = node;
            hashMap.put(pageId, node);
        }

        private boolean full() {
            return hashMap.size() == capacity;
        }
        public void remove(PageId pageId) {
            Node node = hashMap.get(pageId);
            if (tail == node) {
                tail = node.pre;
            }
            if (head == node) {
                head = node.next;
            }
            if (node.pre != null) {
                node.pre.next= node.next;
            }
            if (node.next != null) {
                node.next.pre = node.pre;
            }
            hashMap.remove(pageId);
        }
        public void put(PageId pageId, Page page) {
            try {
                if (hashMap.containsKey(pageId)) {
                    evictPage(pageId);
                }
                if (full()) {
                    evictPage(head.key);
                }
                addLast(pageId, page);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        public Page get(PageId pageId) {
            if (hashMap.containsKey(pageId)) {
                Node node = hashMap.get(pageId);
                remove(pageId);
                addLast(pageId, node.page);
                return node.page;
            }
            return null;
        }

        public boolean containsKey(PageId pageId) {
            return hashMap.containsKey(pageId);
        }
    }
    private LRUCache pageCache;

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pageCache = new LRUCache(numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
        //todo tid和perm还未使用
        if (pageCache.containsKey(pid)) {
            return pageCache.get(pid);
        }
        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pageCache.put(pid, page);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        pages.forEach(page -> {
            page.markDirty(true, tid);
            pageCache.put(page.getId(), page);
        });
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        pages.forEach(page -> {
            page.markDirty(true, tid);
            pageCache.put(page.getId(), page);
        });
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pageId : pageCache.hashMap.keySet()) {
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pageCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if (!pageCache.containsKey(pid)) {
            throw new IOException("the page isn't in the bufferPool");
        }
        Page page = pageCache.get(pid);
        if (page.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    private synchronized void evictPage(PageId pageId) throws IOException {
        flushPage(pageId);
        discardPage(pageId);
    }

}
