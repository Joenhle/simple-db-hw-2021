package simpledb.optimizer.histogram;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.DbFileIterator;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;

import java.util.Arrays;

/**
 * A class to estimate the cardinality the column join of two tables
 * For Any Table a and b，establish two histogram using the a's structure, but owning each counter
 */
public class JointHistogram {

    private int[][] counter;
    private int[] bBorder;
    private int[] sum;
    private int range;
    private int min;
    private int max;

    //constructor for Integer
    public JointHistogram(int buckets, int min, int max) {
        if (max-min+1 < buckets) {
            buckets = max - min + 1;
        }
        this.min = min;
        this.max = max;
        counter = new int[2][buckets];
        range = (max - min + 1) / buckets;
        sum = new int[2];
        bBorder = new int[2];
    }

    //constructor for String
    public JointHistogram(int buckets) {
        this(buckets, StringHistogram.minVal(), StringHistogram.maxVal());
    }

    private int getIndex(int v) {
        if (v >= min && v <= max) {
            v = v - min;
            int index = (int) Math.ceil((double) v / range);
            if (index > counter[0].length-1) {
                index = counter[0].length-1;
            }
            return index;
        } else {
            throw new IllegalArgumentException("the v is out of range");
        }
    }

    private void add(int v, int histogram) {
        if (v < min || v > max) {
            if (histogram == 1) {
                int i = v < min ? 0 : 1;
                bBorder[i]++;
                sum[1]++;
            }
            return;
        }
        int index = getIndex(v);
        counter[histogram][index]++;
        sum[histogram]++;
    }

    public void addValueA(Object v) {
        if (v instanceof Integer) {
            add((int) v, 0);
        } else if (v instanceof String) {
            int temp = StringHistogram.stringToInt((String) v);
            add(temp, 0);
        } else {
            throw new IllegalArgumentException(String.format("the type of v[%s] is not Integer or String", v.getClass().toString()));
        }
    }

    public void addValueB(Object v) {
        if (v instanceof Integer) {
            add((int) v, 1);
        } else if (v instanceof String) {
            int temp = StringHistogram.stringToInt((String) v);
            add(temp, 1);
        } else {
            throw new IllegalArgumentException(String.format("the type of v[%s] is not Integer or String", v.getClass().toString()));
        }
    }

    public void addValueByIterator(DbFileIterator iter1, int column1, DbFileIterator iter2, int column2) {
        DbFileIterator[] iters = new DbFileIterator[]{iter1, iter2};
        int[] columns = new int[]{column1, column2};
        for (int i = 0; i < 2; i++) {
            try {
                iters[i].open();
                iters[i].rewind();
                while (iters[i].hasNext()) {
                    Tuple next = iters[i].next();
                    if (next.getField(columns[i]).getType().equals(Type.INT_TYPE)) {
                        if (i == 0) {
                            addValueA(((IntField)next.getField(columns[i])).getValue());
                        } else {
                            addValueB(((IntField)next.getField(columns[i])).getValue());
                        }
                    } else if (next.getField(columns[i]).getType().equals(Type.STRING_TYPE)) {
                        if (i == 0) {
                            addValueA(((StringField)next.getField(columns[i])).getValue());
                        } else {
                            addValueB(((StringField)next.getField(columns[i])).getValue());
                        }
                    }
                }
                iters[i].close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private int getEqualNum() {
        int sum = 0;
        for (int i = 0; i < counter[0].length; i++) {
            sum += Math.min(counter[0][i], counter[1][i]);
        }
        return sum;
    }

    private int getLessThan() {
        int []right = new int[counter[0].length];
        right[right.length-1] = bBorder[1];
        for (int i = counter[1].length-2; i >= 0; i--) {
            right[i] = right[i+1] + counter[1][i+1];
        }
        int sum = 0;
        for (int i = 0; i < counter[0].length-1; i++) {
            sum += (counter[0][i] * right[i]);
        }
        return sum;
    }

    public int estimateCardinality(Predicate.Op op) {
        switch (op) {
            case EQUALS:
                return getEqualNum();
            case LESS_THAN:
                return getLessThan();
            case NOT_EQUALS:
                return sum[0] * sum[1] - estimateCardinality(Predicate.Op.EQUALS);
            case LESS_THAN_OR_EQ:
                return estimateCardinality(Predicate.Op.EQUALS) + estimateCardinality(Predicate.Op.LESS_THAN);
            case GREATER_THAN:
                return sum[0] * sum[1] - estimateCardinality(Predicate.Op.LESS_THAN_OR_EQ);
            case GREATER_THAN_OR_EQ:
                return estimateCardinality(Predicate.Op.EQUALS) + estimateCardinality(Predicate.Op.GREATER_THAN);
            default:
                throw new IllegalArgumentException("the op mismatch");
        }
    }

    @Override
    public String toString() {
        return "JointHistogram{" +
                "counter=" + Arrays.toString(counter) +
                ", bBorder=" + Arrays.toString(bBorder) +
                ", sum=" + Arrays.toString(sum) +
                ", range=" + range +
                ", min=" + min +
                ", max=" + max +
                '}';
    }
}
