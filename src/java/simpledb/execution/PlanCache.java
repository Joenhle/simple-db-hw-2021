package simpledb.execution;
import simpledb.optimizer.CostCard;
import simpledb.optimizer.LogicalJoinNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A PlanCache is a helper class that can be used to store the best
 * way to order a given set of joins */
public class PlanCache {

    final Map<Set<LogicalJoinNode>, CostCard[]> bestPlan = new HashMap<>();
    public void addPlan(Set<LogicalJoinNode> s, CostCard[] costCards) {
        bestPlan.put(s, costCards);
    }

    public CostCard[] getPlan(Set<LogicalJoinNode> s) {
        return bestPlan.get(s);
    }

    public boolean containsKey(Set<LogicalJoinNode> s) {
        return bestPlan.containsKey(s);
    }
}
