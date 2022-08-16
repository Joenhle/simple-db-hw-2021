package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private int groupByField;
    private Type groupByFiledType;
    private int affectField;
    private Op op;
    // key=-1，存储的是不分组的中间结果
    private HashMap<Object, Integer> innerResMap;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        groupByField = gbfield;
        groupByFiledType = gbfieldtype;
        affectField = afield;
        op = what;
        if (op != Op.COUNT) {
            throw new IllegalArgumentException("Op only supports COUNT");
        }
        innerResMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        boolean noGroup = (groupByField == Aggregator.NO_GROUPING);
        Object key = -1;
        if (!noGroup) {
            Field gbField = tup.getField(groupByField);
            if (groupByFiledType == Type.INT_TYPE) {
                key = ((IntField) gbField).getValue();
            } else {
                key = ((StringField) gbField).getValue();
            }
        }
        innerResMap.put(key, innerResMap.getOrDefault(key, 0) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new StringAggregatorOpIterator();
    }

    private class StringAggregatorOpIterator implements OpIterator {

        private ArrayList<Tuple> tuples;
        private Iterator<Tuple> iter;
        private boolean isOpen;
        public StringAggregatorOpIterator() {
            tuples = new ArrayList<>();
            isOpen = false;
            boolean noGroup = (groupByField == Aggregator.NO_GROUPING);
            if (noGroup) {
                innerResMap.putIfAbsent(-1, 0);
                Tuple temp = new Tuple(getTupleDesc());
                temp.setField(0, new IntField(innerResMap.get(-1)));
                tuples.add(temp);
            } else {
                innerResMap.forEach((key, value) -> {
                    Tuple temp = new Tuple(getTupleDesc());
                    if (groupByFiledType == Type.INT_TYPE) {
                        temp.setField(0, new IntField((Integer) key));
                    } else {
                        temp.setField(0, new StringField((String) key, Type.STRING_LEN));
                    }
                    temp.setField(1, new IntField(value));
                    tuples.add(temp);
                });
            }
            iter = tuples.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new IllegalStateException("the iterator hasn't open");
            }
            return iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen) {
                throw new IllegalStateException("the iterator hasn't open");
            }
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return iter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new IllegalStateException("the iterator hasn't open");
            }
            iter = tuples.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            if (groupByField == Aggregator.NO_GROUPING) {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            } else if (groupByFiledType == Type.INT_TYPE) {
                return new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[]{Type.STRING_TYPE, Type.INT_TYPE});
            }
        }

        @Override
        public void close() {
            isOpen = false;
        }
    }

}
