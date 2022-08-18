package simpledb.storage.evict;

import simpledb.common.Database;
import simpledb.storage.BufferPool;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class LRUCache implements Cache{

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
                Database.getBufferPool().evictPage(pageId);
            }
            if (full()) {
                Database.getBufferPool().evictPage(head.key);
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

    public Iterator<PageId> KeyIterator() {
        return hashMap.keySet().iterator();
    }
}