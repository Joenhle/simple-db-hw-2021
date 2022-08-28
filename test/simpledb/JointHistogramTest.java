package simpledb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import simpledb.common.Database;
import simpledb.execution.Predicate;
import simpledb.optimizer.JoinOptimizer;
import simpledb.optimizer.LogicalJoinNode;
import simpledb.optimizer.TableStats;
import simpledb.storage.HeapFile;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.List;

public class JointHistogramTest extends SimpleDbTestBase {

    HeapFile f1, f2;
    String tableName1, tableName2;
    int tableId1, tableId2;
    TableStats stats1, stats2;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        int[][] range1 = new int[][]{new int[]{0, 99}};
        f1 = SystemTestUtil.createRangeHeapFile(range1, "a");
        tableName1 = "T1";
        Database.getCatalog().addTable(f1, tableName1);
        tableId1 = Database.getCatalog().getTableId(tableName1);
        stats1 = new TableStats(tableId1, 19);
        TableStats.setTableStats(tableName1, stats1);

        int[][] range2 = new int[][]{new int[]{-99, 199}};
        f2 = SystemTestUtil.createRangeHeapFile(range2, "b");
        tableName2 = "T2";
        Database.getCatalog().addTable(f2, tableName2);
        tableId2 = Database.getCatalog().getTableId(tableName2);
        stats2 = new TableStats(tableId2, 19);
        TableStats.setTableStats(tableName2, stats2);

        TableStats.buildJointHistograms();
    }

    private boolean equalEndurable(double actual, double expect, double err) {
        double min = expect * (1 - err), max = expect * (1 + err);
        return actual >= min && actual <= max;
    }

    @Test
    public void estimateJoinCardinality() throws Exception {
        //test equal
        TransactionId tid = new TransactionId();
        Parser p = new Parser();
        JoinOptimizer j = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM " + tableName1 + " t1, " + tableName2 + " t2 WHERE t1.a1 = t2.b1;"), new ArrayList<>());
        double cardinality = j.estimateJoinCardinality(new LogicalJoinNode("t1", "t2", "a"+0, "b"+0, Predicate.Op.EQUALS), stats1.estimateTableCardinality(1), stats2.estimateTableCardinality(1), false, false, TableStats.getStatsMap());
        Assert.assertTrue(equalEndurable(cardinality, 100, 0.05));

        //test less than
        j = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM " + tableName1 + " t1, " + tableName2 + " t2 WHERE t1.a1 <= t2.b1;"), new ArrayList<>());
        cardinality = j.estimateJoinCardinality(new LogicalJoinNode("t1", "t2", "a"+0, "b"+0, Predicate.Op.LESS_THAN_OR_EQ), stats1.estimateTableCardinality(1), stats2.estimateTableCardinality(1), false, false, TableStats.getStatsMap());
        Assert.assertTrue(equalEndurable(cardinality, (100 + 200) * 100 / 2, 0.05));
    }
}
