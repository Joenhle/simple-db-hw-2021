package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.Comparator;

public class MergeJoin extends Operator {

    private OpIterator child1, child2;
    private ArrayList<Tuple> t1Tuples, t2Tuples;
    private int i, j;
    private JoinPredicate predicate;
    public boolean bothBaseTable;

    public MergeJoin(JoinPredicate predicate, OpIterator child1, OpIterator child2) {
        this.child1 = child1;
        this.child2 = child2;
        this.predicate = predicate;
    }

    public JoinPredicate getJoinPredicate() {
        return predicate;
    }

    public String getJoinField1Name()
    {
        return this.child1.getTupleDesc().getFieldName(this.predicate.getField1());
    }

    public String getJoinField2Name()
    {
        return this.child2.getTupleDesc().getFieldName(this.predicate.getField2());
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        child1.open();
        child2.open();
        super.open();
    }

    @Override
    public void close() {
        child1.close();
        child2.close();
        super.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        i = 0;
        j = 0;
    }

    private void sortTuples() throws TransactionAbortedException, DbException {
        t1Tuples = new ArrayList<>();
        t2Tuples = new ArrayList<>();
        while (child1.hasNext()) {
            t1Tuples.add(child1.next());
        }
        while (child2.hasNext()) {
            t2Tuples.add(child2.next());
        }
        Comparator<Tuple> comparator1 = (a, b) -> {
            Field field1 = a.getField(predicate.getField1());
            Field field2 = b.getField(predicate.getField1());
            if (field1.compare(Predicate.Op.LESS_THAN, field2)) {
                return -1;
            } else if (field1.compare(Predicate.Op.EQUALS, field2)) {
                return 0;
            } else {
                return 1;
            }
        };
        Comparator<Tuple> comparator2 = (a, b) -> {
            Field field1 = a.getField(predicate.getField2());
            Field field2 = b.getField(predicate.getField2());
            if (field1.compare(Predicate.Op.LESS_THAN, field2)) {
                return -1;
            } else if (field1.compare(Predicate.Op.EQUALS, field2)) {
                return 0;
            } else {
                return 1;
            }
        };
        t1Tuples.sort(comparator1);
        t2Tuples.sort(comparator2);
    }

    private Tuple getTuple(int i, int j) {
        TupleDesc tupleDesc = getTupleDesc();
        Tuple t1 = t1Tuples.get(i), t2 = t2Tuples.get(j);
        Tuple res = new Tuple(tupleDesc);
        for (int k = 0; k < tupleDesc.numFields(); k++) {
            if (k < t1.getTupleDesc().numFields()) {
                res.setField(k, t1.getField(k));
            } else {
                res.setField(k, t2.getField(k - t1.getTupleDesc().numFields()));
            }
        }
        return res;
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (t1Tuples == null) {
            sortTuples();
        }
        while (i < t1Tuples.size()) {
            if (j == t2Tuples.size()) {
                j = 0;
                i++;
                continue;
            }
            if (predicate.filter(t1Tuples.get(i), t2Tuples.get(j))) {
                return getTuple(i, j++);
            } else {
                switch (predicate.getOperator()) {
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQ:
                        j = t2Tuples.size();
                        continue;
                }
                j++;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child1, child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }

    @Override
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }
}
