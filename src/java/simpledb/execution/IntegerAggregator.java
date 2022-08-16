package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int groupByField;
    private Type groupByFiledType;
    private int affectField;
    private Op op;
    // key=-1，存储的是不分组的中间结果
    private HashMap<Object, InnerRes> innerResMap;
    private class InnerRes {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int sum;
        private int count;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        groupByField = gbfield;
        groupByFiledType = gbfieldtype;
        affectField = afield;
        op = what;
        innerResMap = new HashMap<>();
        if (groupByField == Aggregator.NO_GROUPING) {
            innerResMap.put(-1, new InnerRes());    
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Object gbFieldValue;
        if (groupByFiledType == Type.INT_TYPE) {
            IntField gbField = (IntField) tup.getField(groupByField);
            gbFieldValue = gbField.getValue();
        } else {
            StringField gbField = (StringField) tup.getField(groupByField);
            gbFieldValue = gbField.getValue();
        }
        IntField afField = (IntField) tup.getField(affectField);
        boolean noGroup = (groupByField == Aggregator.NO_GROUPING);
        Object key = noGroup ? -1 : gbFieldValue;
        int value = afField.getValue();
        if (!noGroup && !innerResMap.containsKey(key)) {
            innerResMap.put(key, new InnerRes());
        }
        switch (op) {
            case MIN:
                // no grouping
                innerResMap.get(key).min = Math.min(value, innerResMap.get(key).min);
                break;
            case MAX:
                innerResMap.get(key).max = Math.max(value, innerResMap.get(key).max);
                break;
            case SUM:
                innerResMap.get(key).sum += value;
                break;
            case COUNT:
                innerResMap.get(key).count += 1;
                break;
            case AVG:
                innerResMap.get(key).sum += value;
                innerResMap.get(key).count += 1;
                break;
            default:
                throw new IllegalArgumentException("没有OP对应的聚合操作");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new IntegerAggregatorOpIterator();
    }

    private class IntegerAggregatorOpIterator implements OpIterator {

        private ArrayList<Tuple> tuples;
        private Iterator<Tuple> iter;
        private boolean isOpen;
        public IntegerAggregatorOpIterator() {
            tuples = new ArrayList<>();
            isOpen = false;
            boolean noGroup = (groupByField == Aggregator.NO_GROUPING);
            if (noGroup) {
                Tuple temp = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
                Field field;
                InnerRes value = innerResMap.get(-1);
                switch (op) {
                    case MIN:
                        field = new IntField(value.min);
                        break;
                    case MAX:
                        field = new IntField(value.max);
                        break;
                    case SUM:
                        field = new IntField(value.sum);
                        break;
                    case COUNT:
                        field = new IntField(value.count);
                        break;
                    case AVG:
                        field = new IntField(value.sum/value.count);
                        break;
                    default:
                        throw new IllegalArgumentException("没有OP对应的聚合操作");
                }
                temp.setField(0, field);
                tuples.add(temp);
            } else {
                innerResMap.forEach((key, value) -> {
                    Tuple temp = new Tuple(getTupleDesc());
                    if (groupByFiledType == Type.INT_TYPE) {
                        temp.setField(0, new IntField((Integer) key));
                    } else {
                        temp.setField(0, new StringField((String) key, Type.STRING_LEN));
                    }
                    Field field = null;
                    switch (op) {
                        case MIN:
                            field = new IntField(value.min);
                            break;
                        case MAX:
                            field = new IntField(value.max);
                            break;
                        case SUM:
                            field = new IntField(value.sum);
                            break;
                        case COUNT:
                            field = new IntField(value.count);
                            break;
                        case AVG:
                            field = new IntField(value.sum/value.count);
                            break;
                        default:
                            throw new IllegalArgumentException("没有OP对应的聚合操作");
                    }
                    temp.setField(1, field);
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
                return null;
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
            } else {
                return new TupleDesc(new Type[]{groupByFiledType, Type.INT_TYPE});
            }
        }

        @Override
        public void close() {
            isOpen = false;
        }
    }

}
