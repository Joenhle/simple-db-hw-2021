package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.util.FileUtil;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            byte[] data = FileUtil.readContent(file, pid.getPageNumber() * BufferPool.getPageSize(), BufferPool.getPageSize());
            HeapPage page = new HeapPage((HeapPageId) pid, data);
            return page;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        if (page.getId().getPageNumber() > numPages()) {
            throw new IOException("Page Number exceeds the range of file");
        }
        FileUtil.writeContent(file, page.getId().getPageNumber() * BufferPool.getPageSize(), page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) file.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                return new ArrayList<>(Collections.singletonList(page));
            }
        }
        writePage(new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData()));
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages()-1), Permissions.READ_WRITE);
        page.insertTuple(t);
        return new ArrayList<>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator extends AbstractDbFileIterator {

        private boolean isOpen;
        private int curPid;
        private TransactionId tid;
        private Iterator<Tuple> iter;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            isOpen = false;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                return false;
            }
            while (!iter.hasNext() && curPid < numPages()-1) {
                curPid++;
                HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), curPid), Permissions.READ_ONLY);
                iter = curPage.iterator();
            }
            return iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen) {
                throw new NoSuchElementException();
            }
            Tuple next = readNext();
            if (next == null) {
                throw new NoSuchElementException();
            }
            return next;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            if (iter == null) {
                //HeapFile的iterator应该从BufferPool里面去读取Page
                HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), curPid), Permissions.READ_ONLY);
                iter = curPage.iterator();
            }
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (!hasNext()) {
                return null;
            }
            return iter.next();
        }


        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new DbException("the iterator hasn't open");
            }
            curPid = 0;
            iter = null;
            open();
        }
    }

}



