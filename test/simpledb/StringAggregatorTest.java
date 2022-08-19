package simpledb;

import java.util.*;

import org.junit.Before;
import org.junit.Test;

import simpledb.common.Type;
import simpledb.execution.Aggregator;
import simpledb.execution.OpIterator;
import simpledb.execution.StringAggregator;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.systemtest.SimpleDbTestBase;

import static org.junit.Assert.assertEquals;

import junit.framework.JUnit4TestAdapter;

public class StringAggregatorTest extends SimpleDbTestBase {

    final int width1 = 2;
    OpIterator scan1, scan2;
    int[][] count = null;

    /**
     * Initialize each unit scantest
     */
    @Before
    public void createTupleList() {
        this.scan1 = TestUtil.createTupleList(width1,
                new Object[]{1, "a",
                        1, "b",
                        1, "c",
                        3, "d",
                        3, "e",
                        3, "f",
                        5, "g"});

        this.scan2 = TestUtil.createTupleList(width1,
                new Object[]{"a", 1,
                        "a", 2,
                        "a", 3,
                        "b", 4,
                        "b", 5,
                        "c", 6,
                        "d", 7});

        // verify how the results progress after a few merges
        this.count = new int[][]{
                {1, 1},
                {1, 2},
                {1, 3},
                {1, 3, 3, 1}
        };

    }

    /**
     * Test String.mergeTupleIntoGroup() and iterator() over a COUNT
     */
    @Test
    public void mergeCount() throws Exception {
        scan1.open();
        StringAggregator agg = new StringAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.COUNT);

        for (int[] step : count) {
            agg.mergeTupleIntoGroup(scan1.next());
            OpIterator it = agg.iterator();
            it.open();
            TestUtil.matchAllTuples(TestUtil.createTupleList(width1, step), it);
        }
    }

    @Test
    public void mergeCountString() throws Exception {
        scan2.open();
        StringAggregator agg = new StringAggregator(0, Type.STRING_TYPE, 1, Aggregator.Op.COUNT);
        while (scan2.hasNext()) {
            agg.mergeTupleIntoGroup(scan2.next());
        }
        OpIterator it = agg.iterator();
        it.open();
        String[] arr1 = new String[]{"a", "b", "c", "d"};
        int[] arr2 = new int[]{3, 2, 1, 1};
        int i = 0;
        while (it.hasNext()) {
            Tuple next = it.next();
            StringField gbField = (StringField) next.getField(0);
            IntField vaField = (IntField) next.getField(1);
            assertEquals(arr1[i], gbField.getValue());
            assertEquals(arr2[i++], vaField.getValue());
        }
    }

    @Test
    public void mergeCountNoGroup() throws Exception {
        scan2.open();
        StringAggregator agg = new StringAggregator(Aggregator.NO_GROUPING, null, 1, Aggregator.Op.COUNT);
        while (scan2.hasNext()) {
            agg.mergeTupleIntoGroup(scan2.next());
        }
        OpIterator it = agg.iterator();
        it.open();
        IntField countField = (IntField) it.next().getField(0);
        assertEquals(7, countField.getValue());
    }


    /**
     * Test StringAggregator.iterator() for OpIterator behaviour
     */
    @Test
    public void testIterator() throws Exception {
        // first, populate the aggregator via sum over scan1
        scan1.open();
        StringAggregator agg = new StringAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.COUNT);
        try {
            while (true)
                agg.mergeTupleIntoGroup(scan1.next());
        } catch (NoSuchElementException e) {
            // explicitly ignored
        }

        OpIterator it = agg.iterator();
        it.open();

        // verify it has three elements
        int count = 0;
        try {
            while (true) {
                it.next();
                count++;
            }
        } catch (NoSuchElementException e) {
            // explicitly ignored
        }
        assertEquals(3, count);

        // rewind and try again
        it.rewind();
        count = 0;
        try {
            while (true) {
                it.next();
                count++;
            }
        } catch (NoSuchElementException e) {
            // explicitly ignored
        }
        assertEquals(3, count);

        // close it and check that we don't get anything
        it.close();
        try {
            it.next();
            throw new Exception("StringAggreator iterator yielded tuple after close");
        } catch (Exception e) {
            // explicitly ignored
        }
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(StringAggregatorTest.class);
    }
}

