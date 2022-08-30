package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.ParsingException;
import simpledb.execution.*;
import simpledb.optimizer.histogram.JointHistogram;
import simpledb.storage.TupleDesc;

import java.util.*;

import javax.swing.*;
import javax.swing.tree.*;

/**
 * The JoinOptimizer class is responsible for ordering a series of joins
 * optimally, and for selecting the best instantiation of a join for a given
 * logical plan.
 */
public class JoinOptimizer {
    final LogicalPlan p;
    final List<LogicalJoinNode> joins;

    /**
     * Constructor
     * 
     * @param p
     *            the logical plan being optimized
     * @param joins
     *            the list of joins being performed
     */
    public JoinOptimizer(LogicalPlan p, List<LogicalJoinNode> joins) {
        this.p = p;
        this.joins = joins;
    }

    /**
     * Return best iterator for computing a given logical join, given the
     * specified statistics, and the provided left and right subplans. Note that
     * there is insufficient information to determine which plan should be the
     * inner/outer here -- because OpIterator's don't provide any cardinality
     * estimates, and stats only has information about the base tables. For this
     * reason, the plan1
     * 
     * @param lj
     *            The join being considered
     * @param plan1
     *            The left join node's child
     * @param plan2
     *            The right join node's child
     */
    public static OpIterator instantiateJoin(LogicalJoinNode lj, OpIterator plan1, OpIterator plan2) throws ParsingException {

        int t1id = 0, t2id = 0;
        OpIterator j;

        try {
            t1id = plan1.getTupleDesc().fieldNameToIndex(lj.f1QuantifiedName);
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown field " + lj.f1QuantifiedName);
        }

        if (lj instanceof LogicalSubplanJoinNode) {
            t2id = 0;
        } else {
            try {
                t2id = plan2.getTupleDesc().fieldNameToIndex(
                        lj.f2QuantifiedName);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field "
                        + lj.f2QuantifiedName);
            }
        }

        JoinPredicate p = new JoinPredicate(t1id, lj.p, t2id);
        boolean bothBaseTable = plan1 instanceof SeqScan && plan2 instanceof SeqScan;
        switch (lj.joinMethod) {
            case NestedLoop:
                j = new Join(p, plan1, plan2);
                /** logicalQuery plan 向execute operator{Join,HashJoin,MergeJoin}传递信息，方便{@link OperatorCardinality}进行更新cardinality*/
                ((Join) j).bothBaseTable = bothBaseTable;
                break;
            case HashJoin:
                j = new HashEquiJoin(p, plan1, plan2);
                ((HashEquiJoin) j).bothBaseTable = bothBaseTable;
                break;
            case MergeJoin:
                j = new MergeJoin(p, plan1, plan2);
                ((MergeJoin) j).bothBaseTable = bothBaseTable;
                break;
            default:
                throw new IllegalArgumentException("No such join method");
        }
        return j;

    }

    /**
     * Estimate the cost of a join.
     * 
     * The cost of the join should be calculated based on the join algorithm (or
     * algorithms) that you implemented for Lab 2. It should be a function of
     * the amount of data that must be read over the course of the query, as
     * well as the number of CPU opertions performed by your join. Assume that
     * the cost of a single predicate application is roughly 1.
     * 
     * 
     * @param j
     *            A LogicalJoinNode representing the join operation being
     *            performed.
     * @param card1
     *            Estimated cardinality of the left-hand side of the query
     * @param card2
     *            Estimated cardinality of the right-hand side of the query
     * @param cost1
     *            Estimated cost of one full scan of the table on the left-hand
     *            side of the query
     * @param cost2
     *            Estimated cost of one full scan of the table on the right-hand
     *            side of the query
     * @return An estimate of the cost of this query, in terms of cost1 and
     *         cost2
     */
    public double estimateJoinCost(LogicalJoinNode j, int card1, int card2, double cost1, double cost2) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1 + cost1 + cost2;
        } else {
            // Insert your code here.
            // HINT: You may need to use the variable "j" if you implemented
            // a join algorithm that's more complicated than a basic
            // nested-loops join.
            double[] nestedLoopJoinCost = new double[]{cost1 + card1 * cost2 + card1 * card2, 0};
            double[] hashJoinCost = new double[]{cost1 + cost2 + card1 + card2, 1};
            double[] mergeJoinCost = new double[]{cost1 + cost2 + card1*Math.log(card1) + card2*Math.log(card2) + (double) card1*card1/2, 2};
            double[][] temp = new double[][]{nestedLoopJoinCost, hashJoinCost, mergeJoinCost};
            Arrays.sort(temp, (a, b) -> {return Double.compare(a[0], b[0]);});
            if (j.p.equals(Predicate.Op.EQUALS)) {
                j.joinMethod = JoinMethod.get((int) temp[0][1]);
                return temp[0][0];
            } else {
                int best = 0;
                if (temp[0][1] == 1) {
                    best = 1;
                }
                j.joinMethod = JoinMethod.get((int) temp[best][1]);
                return temp[best][0];
            }
        }
    }

    public int estimateJoinCardinality(boolean bothBaseTable, LogicalJoinNode j, int card1, int card2, boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1;
        } else {
            return estimateTableJoinCardinality(bothBaseTable, j.p, j.t1Alias, j.t2Alias, j.f1PureName, j.f2PureName, card1, card2, t1pkey, t2pkey, stats, p.getTableAliasToIdMapping());
        }
    }

    /**
     * Estimate the cardinality of a join. The cardinality of a join is the
     * number of tuples produced by the join.
     * 
     * @param j
     *            A LogicalJoinNode representing the join operation being
     *            performed.
     * @param card1
     *            Cardinality of the left-hand table in the join
     * @param card2
     *            Cardinality of the right-hand table in the join
     * @param t1pkey
     *            Is the left-hand table a primary-key table?
     * @param t2pkey
     *            Is the right-hand table a primary-key table?
     * @param stats
     *            The table stats, referenced by table names, not alias
     * @return The cardinality of the join
     */
    public int estimateJoinCardinality(LogicalJoinNode j, int card1, int card2, boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1;
        } else {
            return estimateTableJoinCardinality(true, j.p, j.t1Alias, j.t2Alias, j.f1PureName, j.f2PureName, card1, card2, t1pkey, t2pkey, stats, p.getTableAliasToIdMapping());
        }
    }

    /**
     * Estimate the join cardinality of two tables.
     * */
    public static int estimateTableJoinCardinality(boolean bothBaseTable, Predicate.Op joinOp, String table1Alias, String table2Alias, String field1PureName, String field2PureName, int card1, int card2, boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats, Map<String, Integer> tableAliasToId) {

        String tableName1 = Database.getCatalog().getTableName(tableAliasToId.get(table1Alias)),
                tableName2 = Database.getCatalog().getTableName(tableAliasToId.get(table2Alias));

        JointHistogram jointHistogram = TableStats.getJointHistogram(tableName1, tableName2, field1PureName, field2PureName);
        if (bothBaseTable && jointHistogram != null) {
            int cardinality = jointHistogram.estimateCardinality(joinOp);
            System.out.println(String.format("JointHistogram success, the jointHistogram cardinality of %s.%s %s %s.%s is %d", tableName1, field1PureName, joinOp.toString(), tableName2, field2PureName, cardinality));
            if (t1pkey && !t2pkey) {
                return Math.min(cardinality, card2);
            } else if (!t1pkey && t2pkey) {
                return Math.min(cardinality, card1);
            } else if (t1pkey && t2pkey) {
                return Math.min(cardinality, Math.min(card1, card2));
            }
            return cardinality;
        } else {
            if (joinOp.equals(Predicate.Op.EQUALS)) {
                //todo 这里可以用C(A,S)来优化，参考CMU15445 Optimizer-2节的方法
                if (t1pkey && !t2pkey) {
                    return card2;
                } else if (!t1pkey && t2pkey) {
                    return card1;
                } else if (t1pkey && t2pkey) {
                    return Math.min(card1, card2);
                }
                return Math.max(card1, card2);
            } else if (joinOp.equals(Predicate.Op.NOT_EQUALS)) {
                return card1 * card2 - estimateTableJoinCardinality(bothBaseTable, Predicate.Op.EQUALS, table1Alias, table2Alias, field1PureName, field2PureName, card1, card2, t1pkey, t2pkey, stats, tableAliasToId);
            } else {
                return (int) (0.3 * card1 * card2);
            }
        }

    }

    /**
     * Helper method to enumerate all of the subsets of a given size of a
     * specified vector.
     * 
     * @param v
     *            The vector whose subsets are desired
     * @param size
     *            The size of the subsets of interest
     * @return a set of all subsets of the specified size
     */
    public <T> Set<Set<T>> enumerateSubsets(List<T> v, int size) {
        Set<Set<T>> res = new HashSet<>();
        Set<T> temp = new HashSet<>();
        int[] arr = new int[size];
        int level = 0;
        while (true) {
            while (arr[level] < v.size()) {
                if (level == size-1) {
                    temp.clear();
                    for (int i = 0; i < arr.length; i++) {
                        temp.add(v.get(arr[i]));
                    }
                    res.add(new HashSet<>(temp));
                    arr[level]++;
                } else {
                    level++;
                    arr[level] = arr[level-1] + 1;
                }
            }
            if (--level < 0) {
                break;
            }
            arr[level]++;
        }
        return res;
    }

    /**
     * Compute a logical, reasonably efficient join on the specified tables. See
     * PS4 for hints on how this should be implemented.
     * 
     * @param stats
     *            Statistics for each table involved in the join, referenced by
     *            base table names, not alias
     * @param filterSelectivities
     *            Selectivities of the filter predicates on each table in the
     *            join, referenced by table alias (if no alias, the base table
     *            name)
     * @param explain
     *            Indicates whether your code should explain its query plan or
     *            simply execute it
     * @return A List<LogicalJoinNode> that stores joins in the left-deep
     *         order in which they should be executed.
     * @throws ParsingException
     *             when stats or filter selectivities is missing a table in the
     *             join, or or when another internal error occurs
     */
    public List<LogicalJoinNode> orderJoins(Map<String, TableStats> stats, Map<String, Double> filterSelectivities, boolean explain) throws ParsingException {
        if (joins.size() == 0) {
            return joins;
        }

        PlanCache planCache = new PlanCache();
        for (int i = 1; i <= joins.size(); i++) {
            for (Set<LogicalJoinNode> s : enumerateSubsets(joins, i)) {
                CostCard[] bestPlan = null;
                for (LogicalJoinNode node : s) {
                    CostCard[] costCard = computeCostAndCardOfSubplan(stats, filterSelectivities, node, s, bestPlan, planCache);
                    if (costCard != null) {
                        bestPlan = costCard;
                    }
                }
                planCache.addPlan(s, bestPlan);
            }
        }
        CostCard[] bestPlan = planCache.getPlan(new HashSet<>(joins));
        List<LogicalJoinNode> orderedJoins = null;
        if (bestPlan.length == 1 && (orderedJoins = bestPlan[0].plan) != null) {
            if (explain) {
                printJoins(orderedJoins, planCache, stats, filterSelectivities);
            }
            return orderedJoins;
        }
        return joins;
    }

    // ===================== Private Methods =================================

    private CostCard swapAndCompareJoinCost(List<LogicalJoinNode> prevBestPlan, LogicalJoinNode j, int card1, int card2, double cost1, double cost2, boolean leftPkey, boolean rightPkey, boolean bothBaseTable, Map<String, TableStats> stats) {
        double joinCost1 = estimateJoinCost(j, card1, card2, cost1, cost2);
        LogicalJoinNode j2 = j.swapInnerOuter();
        double joinCost2 = estimateJoinCost(j2, card2, card1, cost2, cost1);
        if (joinCost2 < joinCost1) {
            boolean tmp;
            j = j2;
            joinCost1 = joinCost2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
        }
        CostCard cc = new CostCard();
        cc.card = estimateJoinCardinality(bothBaseTable, j, card1, card2, leftPkey, rightPkey, stats);
        cc.cost = joinCost1;
        cc.plan = new ArrayList<>(prevBestPlan);
        cc.plan.add(j);
        return cc;
    }

    private CostCard getSingleNode(Map<String, TableStats> stats, Map<String, Double> filterSelectivities, LogicalJoinNode j) {
        String table1Name = Database.getCatalog().getTableName(this.p.getTableId(j.t1Alias));
        String table2Name = Database.getCatalog().getTableName(this.p.getTableId(j.t2Alias));
        double t1cost = stats.get(table1Name).estimateScanCost();
        int t1card = stats.get(table1Name).estimateTableCardinality(filterSelectivities.get(j.t1Alias));
        boolean leftPkey = isPkey(j.t1Alias, j.f1PureName);
        double t2cost = j.t2Alias == null ? 0 : stats.get(table2Name).estimateScanCost();
        int t2card = j.t2Alias == null ? 0 : stats.get(table2Name).estimateTableCardinality(filterSelectivities.get(j.t2Alias));
        boolean rightPkey = j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName);
        CostCard singleNode = swapAndCompareJoinCost(new ArrayList<>(), j, t1card, t2card, t1cost, t2cost, leftPkey, rightPkey, true, stats);
        return singleNode;
    }

    private CostCard[] mergeJoinNode(Map<String, TableStats> stats, Map<String, Double> filterSelectivities, CostCard[] nodes, LogicalJoinNode j) {
        String table1Name = Database.getCatalog().getTableName(this.p.getTableId(j.t1Alias));
        String table2Name = Database.getCatalog().getTableName(this.p.getTableId(j.t2Alias));
        String table1Alias = j.t1Alias;
        String table2Alias = j.t2Alias;
        double t1cost, t2cost;
        boolean leftPkey, rightPkey;
        int t1card, t2card;
        if (nodes == null || nodes.length == 0) {
            throw new IllegalArgumentException("the length of nodes should be grater than 0");
        }
        if (nodes.length == 1) {
            CostCard node1 = nodes[0];
            if (doesJoin(node1.plan, table1Alias)) {
                t1cost = node1.cost;
                t1card = node1.card;
                leftPkey = hasPkey(node1.plan);
                t2cost = j.t2Alias == null ? 0 : stats.get(table2Name).estimateScanCost();
                t2card = j.t2Alias == null ? 0 : stats.get(table2Name).estimateTableCardinality(filterSelectivities.get(j.t2Alias));
                rightPkey = j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName);
                CostCard merge = swapAndCompareJoinCost(node1.plan, j, t1card, t2card, t1cost, t2cost, leftPkey, rightPkey, false, stats);
                return new CostCard[]{merge};
            }
            if (doesJoin(node1.plan, table2Alias)) {
                t2cost = node1.cost; // left side just has cost of whatever
                t2card = node1.card;
                rightPkey = hasPkey(node1.plan);
                t1cost = stats.get(table1Name).estimateScanCost();
                t1card = stats.get(table1Name).estimateTableCardinality(filterSelectivities.get(j.t1Alias));
                leftPkey = isPkey(j.t1Alias, j.f1PureName);
                CostCard merge = swapAndCompareJoinCost(node1.plan, j, t1card, t2card, t1cost, t2cost, leftPkey, rightPkey, false, stats);
                return new CostCard[]{merge};
            }
            CostCard node2 = getSingleNode(stats, filterSelectivities, j);
            return new CostCard[]{node1, node2};
        } else {
            for (int m = 0; m < nodes.length; m++) {
                for (int n = 0; n < nodes.length; n++) {
                    CostCard node1 = nodes[m], node2 = nodes[n];
                    if (doesJoin(node1.plan, table1Alias) && doesJoin(node2.plan, table2Alias)) {
                        List<LogicalJoinNode> prev = new ArrayList<>(node1.plan);
                        prev.addAll(node2.plan);
                        leftPkey = isPkey(j.t1Alias, j.f1PureName);
                        rightPkey = isPkey(j.t2Alias, j.f2PureName);
                        CostCard merge = swapAndCompareJoinCost(prev, j, node1.card, node2.card, node1.cost, node2.cost, leftPkey, rightPkey, false, stats);
                        CostCard[] res = new CostCard[nodes.length-1];
                        int k = 0;
                        for (int i = 0; i < nodes.length; i++) {
                            if (i != m && i != n) {
                                res[k++] = nodes[i];
                            }
                        }
                        res[res.length-1] = merge;
                        return res;
                    }
                }
            }
            CostCard singleNode = getSingleNode(stats, filterSelectivities, j);
            CostCard[] res = new CostCard[nodes.length + 1];
            System.arraycopy(nodes, 0, res, 0, nodes.length);
            res[res.length - 1] = singleNode;
            return res;
        }
    }

    /**
     * This is a helper method that computes the cost and cardinality of joining
     * joinToRemove to joinSet (joinSet should contain joinToRemove), given that
     * all of the subsets of size joinSet.size() - 1 have already been computed
     * and stored in PlanCache pc.
     *
     * @param stats
     *            table stats for all of the tables, referenced by table names
     *            rather than alias (see {@link #orderJoins})
     * @param filterSelectivities
     *            the selectivities of the filters over each of the tables
     *            (where tables are indentified by their alias or name if no
     *            alias is given)
     * @param joinToRemove
     *            the join to remove from joinSet
     * @param joinSet
     *            the set of joins being considered
     * @param bestCostSoFar
     *            the best way to join joinSet so far (minimum of previous
     *            invocations of computeCostAndCardOfSubplan for this joinSet,
     *            from returned CostCard)
     * @param pc
     *            the PlanCache for this join; should have subplans for all
     *            plans of size joinSet.size()-1
     * @return A {@link CostCard} objects desribing the cost, cardinality,
     *         optimal subplan
     * @throws ParsingException
     *             when stats, filterSelectivities, or pc object is missing
     *             tables involved in join
     */
    private CostCard[] computeCostAndCardOfSubplan(Map<String, TableStats> stats, Map<String, Double> filterSelectivities, LogicalJoinNode joinToRemove, Set<LogicalJoinNode> joinSet, CostCard[] bestCostSoFar, PlanCache pc) throws ParsingException {
        LogicalJoinNode j = joinToRemove;
        if (this.p.getTableId(j.t1Alias) == null)
            throw new ParsingException("Unknown table " + j.t1Alias);
        if (this.p.getTableId(j.t2Alias) == null)
            throw new ParsingException("Unknown table " + j.t2Alias);
        Set<LogicalJoinNode> news = new HashSet<>(joinSet);
        news.remove(j);
        if (news.isEmpty()) { //base case -- both are base relations
            CostCard singleNode = getSingleNode(stats, filterSelectivities, j);
            if (bestCostSoFar != null && singleNode.cost >= bestCostSoFar[0].cost) {
                return null;
            }
            return new CostCard[]{singleNode};
        }
        CostCard[] nodes = pc.getPlan(news);
        if (nodes == null) {
            return null;
        }
        CostCard[] merge = mergeJoinNode(stats, filterSelectivities, nodes, j);
        if (bestCostSoFar != null && ((merge.length == 1 && bestCostSoFar.length == 1 && merge[0].cost >= bestCostSoFar[0].cost) || (merge.length > bestCostSoFar.length))) {
            return null;
        }
        return merge;
    }

    /**
     * Return true if the specified table is in the list of joins, false
     * otherwise
     */
    private boolean doesJoin(List<LogicalJoinNode> joinlist, String table) {
        for (LogicalJoinNode j : joinlist) {
            if (j.t1Alias.equals(table)
                    || (j.t2Alias != null && j.t2Alias.equals(table)))
                return true;
        }
        return false;
    }

    /**
     * Return true if field is a primary key of the specified table, false
     * otherwise
     * 
     * @param tableAlias
     *            The alias of the table in the query
     * @param field
     *            The pure name of the field
     */
    private boolean isPkey(String tableAlias, String field) {
        int tid1 = p.getTableId(tableAlias);
        String pkey1 = Database.getCatalog().getPrimaryKey(tid1);

        return pkey1.equals(field);
    }

    /**
     * Return true if a primary key field is joined by one of the joins in
     * joinlist
     */
    private boolean hasPkey(List<LogicalJoinNode> joinlist) {
        for (LogicalJoinNode j : joinlist) {
            if (isPkey(j.t1Alias, j.f1PureName)
                    || (j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName)))
                return true;
        }
        return false;

    }

    /**
     * Helper function to display a Swing window with a tree representation of
     * the specified list of joins. See {@link #orderJoins}, which may want to
     * call this when the analyze flag is true.
     * 
     * @param js
     *            the join plan to visualize
     * @param pc
     *            the PlanCache accumulated whild building the optimal plan
     * @param stats
     *            table statistics for base tables
     * @param selectivities
     *            the selectivities of the filters over each of the tables
     *            (where tables are indentified by their alias or name if no
     *            alias is given)
     */
    private void printJoins(List<LogicalJoinNode> js, PlanCache pc,
            Map<String, TableStats> stats,
            Map<String, Double> selectivities) {

        JFrame f = new JFrame("Join Plan for " + p.getQuery());

        // Set the default close operation for the window,
        // or else the program won't exit when clicking close button
        f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        f.setVisible(true);

        f.setSize(300, 500);

        Map<String, DefaultMutableTreeNode> m = new HashMap<>();

        // int numTabs = 0;

        // int k;
        DefaultMutableTreeNode root = null, treetop = null;
        HashSet<LogicalJoinNode> pathSoFar = new HashSet<>();
        boolean neither;

        System.out.println(js);
        for (LogicalJoinNode j : js) {
            pathSoFar.add(j);
            System.out.println("PATH SO FAR = " + pathSoFar);

            String table1Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t1Alias));
            String table2Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t2Alias));

            // Double c = pc.getCost(pathSoFar);
            neither = true;

            CostCard node = pc.getPlan(pathSoFar)[0];

            root = new DefaultMutableTreeNode("Join " + j + " (Cost ="
                    + node.cost + ", card = "
                    + node.card+ ")");
            DefaultMutableTreeNode n = m.get(j.t1Alias);
            if (n == null) { // never seen this table before
                n = new DefaultMutableTreeNode(j.t1Alias
                        + " (Cost = "
                        + stats.get(table1Name).estimateScanCost()
                        + ", card = "
                        + stats.get(table1Name).estimateTableCardinality(
                                selectivities.get(j.t1Alias)) + ")");
                root.add(n);
            } else {
                // make left child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t1Alias, root);

            n = m.get(j.t2Alias);
            if (n == null) { // never seen this table before

                n = new DefaultMutableTreeNode(
                        j.t2Alias == null ? "Subplan"
                                : (j.t2Alias
                                        + " (Cost = "
                                        + stats.get(table2Name)
                                                .estimateScanCost()
                                        + ", card = "
                                        + stats.get(table2Name)
                                                .estimateTableCardinality(
                                                        selectivities
                                                                .get(j.t2Alias)) + ")"));
                root.add(n);
            } else {
                // make right child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t2Alias, root);

            // unless this table doesn't join with other tables,
            // all tables are accessed from root
            if (!neither) {
                for (String key : m.keySet()) {
                    m.put(key, root);
                }
            }

            treetop = root;
        }

        JTree tree = new JTree(treetop);
        JScrollPane treeView = new JScrollPane(tree);

        tree.setShowsRootHandles(true);

        // Set the icon for leaf nodes.
        ImageIcon leafIcon = new ImageIcon("join.jpg");
        DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
        renderer.setOpenIcon(leafIcon);
        renderer.setClosedIcon(leafIcon);

        tree.setCellRenderer(renderer);

        f.setSize(300, 500);

        f.add(treeView);
        for (int i = 0; i < tree.getRowCount(); i++) {
            tree.expandRow(i);
        }

        if (js.size() == 0) {
            f.add(new JLabel("No joins in plan."));
        }

        f.pack();

    }

}
