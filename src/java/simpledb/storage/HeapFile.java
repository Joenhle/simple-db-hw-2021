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
            byte[] data = FileUtil.getContent(file, pid.getPageNumber() * BufferPool.getPageSize(), BufferPool.getPageSize());
            HeapPage page = new HeapPage((HeapPageId) pid, data);
            return page;
        } catch (IOException e) {
            e.printStackTrace();
            System.err.print(e.getMessage());
            System.exit(1);
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) file.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

class HeapFileIterator extends AbstractDbFileIterator {
    private final ArrayList<Tuple> tuples = new ArrayList<>();
    private int cursor;
    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        try {
            int numPage = heapFile.numPages();
            for (int i = 0; i < numPage; i++) {
                PageId pid = new HeapPageId(heapFile.getId(), i);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                Iterator<Tuple> iter = page.iterator();
                while (iter.hasNext()) {
                    tuples.add(iter.next());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getMessage());
            System.exit(1);
        }
        this.cursor = -1;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return cursor >= 0 && cursor < tuples.size();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (cursor < 0 || cursor >= tuples.size()) {
            throw new NoSuchElementException();
        }
        return tuples.get(cursor++);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        cursor = 0;
    }

    @Override
    public void close() {
        cursor = -1;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        cursor = 0;
    }
}

