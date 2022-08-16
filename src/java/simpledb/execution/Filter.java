package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private Predicate predicate;
    private OpIterator iter;
    private OpIterator[] children;
    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        predicate = p;
        iter = child;
        // 每个Operator子类的子iterator都在初始化时open，对象本身的open和close直接通过父类调用，且private iter也能保证安全
        try {
            iter.open();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        return iter.getTupleDesc();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iter.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
        while (iter.hasNext()) {
            Tuple next = iter.next();
            if (predicate.filter(next)) {
                return next;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.children = children;
    }

}
