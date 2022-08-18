package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.Iterator;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId tid;
    private OpIterator child;
    private boolean hasDeleted;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        setChildren(new OpIterator[]{child});
        tid = t;
    }
    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }
    public void close() {
        child.close();
        super.close();
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasDeleted) {
            return null;
        }
        Tuple tuple = new Tuple(getTupleDesc());
        int count = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(tid, child.next());
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        tuple.setField(0, new IntField(count));
        hasDeleted = true;
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (child != children[0]) {
            child = children[0];
        }
    }

}
