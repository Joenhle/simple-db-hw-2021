package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.index.BTreeFile;
import simpledb.optimizer.histogram.IntHistogram;
import simpledb.optimizer.histogram.JointHistogram;
import simpledb.optimizer.histogram.StringHistogram;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 *
 * design idea:
 * 1.
 */

public class TableStats {

    private DbFile dbFile;
    private int ioCostPerPage;
    private int ntups;
    private int[][] columnValueMinMax;
    private ArrayList singleHistograms;

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        buildJointHistograms();
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage = ioCostPerPage;
        this.singleHistograms = new ArrayList();
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.columnValueMinMax = new int[Database.getCatalog().getTupleDesc(tableid).numFields()][2];
        buildSingleHistograms(tableid);
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        if (dbFile instanceof HeapFile) {
            return ((HeapFile) dbFile).numPages() * ioCostPerPage;
        } else if (dbFile instanceof BTreeFile) {
            return ((BTreeFile) dbFile).numPages() * ioCostPerPage;
        } else {
            throw new IllegalStateException(String.format("the DBFile's Class[%s] doesn't support numPages method", dbFile.getClass()));
        }
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (ntups * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        if (dbFile.getTupleDesc().getFieldType(field).equals(Type.INT_TYPE)) {
            return ((IntHistogram)singleHistograms.get(field)).avgSelectivity();
        } else {
            return ((StringHistogram)singleHistograms.get(field)).avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (singleHistograms.get(field).getClass().equals(IntHistogram.class) && constant.getType().equals(Type.INT_TYPE)) {
            IntHistogram intHistogram = (IntHistogram) singleHistograms.get(field);
            return intHistogram.estimateSelectivity(op, ((IntField)constant).getValue());
        } else if (singleHistograms.get(field).getClass().equals(StringHistogram.class) && constant.getType().equals(Type.STRING_TYPE)) {
            StringHistogram stringHistogram = (StringHistogram) singleHistograms.get(field);
            return stringHistogram.estimateSelectivity(op, ((StringField)constant).getValue());
        }
        throw new IllegalArgumentException(String.format("the histogram's type[%s] mismatch constant's type[%s]",
                singleHistograms.get(field).getClass().toString(), constant.getType().toString()));
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return ntups;
    }
    
    //single histogram method
    private void buildSingleHistograms(int tableId) {
        //set property columnValueMinMax
        DbFileIterator iter = dbFile.iterator(new TransactionId());
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableId);
        try {
            iter.open();
            while (iter.hasNext()) {
                Tuple next = iter.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                        int num = ((IntField)next.getField(i)).getValue();
                        columnValueMinMax[i][0] = Math.min(columnValueMinMax[i][0], num);
                        columnValueMinMax[i][1] = Math.max(columnValueMinMax[i][1], num);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //build single column histogram
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                singleHistograms.add(new IntHistogram(NUM_HIST_BINS, columnValueMinMax[i][0], columnValueMinMax[i][1]));
            } else {
                singleHistograms.add(new StringHistogram(NUM_HIST_BINS));
            }
        }

        //insert value into single histogram
        try {
            iter.rewind();
            while (iter.hasNext()) {
                Tuple next = iter.next();
                ntups++;
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                        IntHistogram intHistogram = (IntHistogram) singleHistograms.get(i);
                        intHistogram.addValue(((IntField)next.getField(i)).getValue());
                    } else {
                        StringHistogram stringHistogram = (StringHistogram) singleHistograms.get(i);
                        stringHistogram.addValue(((StringField)next.getField(i)).getValue());
                    }
                }
            }
            iter.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int[][] getColumnValueMinMax() {
        return columnValueMinMax;
    }

    //joint histogram
    private static ConcurrentHashMap<String, JointHistogram> jointHistogramMap = new ConcurrentHashMap<>();

    public static JointHistogram getJointHistogram(String table1Alias, String table2Alias, String field1PureName, String field2PureName) {
        String key = String.format("%s.%s_%s.%s", table1Alias, field1PureName, table2Alias, field2PureName);
        return jointHistogramMap.get(key);
    }

    public static void buildJointHistograms() {
        Iterator<Integer> idIter = Database.getCatalog().tableIdIterator();
        List<Integer> tableIdArr = new ArrayList<>();
        while (idIter.hasNext()) {
            tableIdArr.add(idIter.next());
        }
        for (int i = 0; i < tableIdArr.size(); i++) {
            for (int j = i; j < tableIdArr.size(); j++) {
                int tableId1 = tableIdArr.get(i), tableId2 = tableIdArr.get(j);
                String tName1 = Database.getCatalog().getTableName(tableId1), tName2 = Database.getCatalog().getTableName(tableId2);
                TupleDesc tDesc1 = Database.getCatalog().getTupleDesc(tableId1), tDesc2 = Database.getCatalog().getTupleDesc(tableId2);
                DbFileIterator iter1 = Database.getCatalog().getDatabaseFile(tableId1).iterator(new TransactionId()),
                        iter2 = Database.getCatalog().getDatabaseFile(tableId2).iterator(new TransactionId());
                int [][] minMax1 = TableStats.getTableStats(tName1).getColumnValueMinMax(),
                        minMax2 = TableStats.getTableStats(tName2).getColumnValueMinMax();
                for (int m = 0; m < tDesc1.numFields(); m++) {
                    for (int n = 0; n < tDesc2.numFields(); n++) {
                        if (tDesc1.getFieldType(m) == tDesc2.getFieldType(n)) {
                            String key1 = String.format("%s.%s_%s.%s", tName1, tDesc1.getFieldName(m), tName2, tDesc2.getFieldName(n));
                            String key2 = String.format("%s.%s_%s.%s", tName2, tDesc2.getFieldName(n), tName1, tDesc1.getFieldName(m));
                            JointHistogram joint;
                            if (tDesc1.getFieldType(m).equals(Type.INT_TYPE)) {
                                //构建new -> old joint
                                joint = new JointHistogram(TableStats.NUM_HIST_BINS, minMax1[m][0], minMax1[m][1]);
                                joint.addValueByIterator(iter1, m, iter2, n);
                                jointHistogramMap.put(key1, joint);
                                //构建old -> new joint
                                joint = new JointHistogram(TableStats.NUM_HIST_BINS, minMax2[n][0], minMax2[n][1]);
                                joint.addValueByIterator(iter2, n, iter1, m);
                                jointHistogramMap.put(key2, joint);
                            } else {
                                //构建new -> old joint
                                joint = new JointHistogram(TableStats.NUM_HIST_BINS);
                                joint.addValueByIterator(iter1, m, iter2, n);
                                jointHistogramMap.put(key1, joint);
                                //构建old -> new joint
                                joint = new JointHistogram(TableStats.NUM_HIST_BINS);
                                joint.addValueByIterator(iter2, n, iter1, m);
                                jointHistogramMap.put(key2, joint);
                            }
                        }
                    }
                }
            }
        }

    }

}
