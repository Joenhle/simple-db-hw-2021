package simpledb.optimizer.histogram;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] bucketNum;
    private int sum;
    private int range;
    private int min;
    private int max;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        if (max-min+1 < buckets) {
            buckets = max-min+1;
        }
        this.min = min;
        this.max = max;
        bucketNum = new int[buckets];
        range = (max-min+1) / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v >= min && v <= max) {
            v = v - min;
            int index = (int) Math.ceil((double) v / range);
            if (index > bucketNum.length-1) {
                index = bucketNum.length-1;
            }
            bucketNum[index]++;
            sum++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (v < min || v > max) {
            int res = v < min ? 0 : 1;
            switch (op) {
                case EQUALS:
                    return 0;
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    return res;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    return 1-res;
            }
        }
        int temp = v - min;
        int width = range;
        int index = (int) Math.ceil((double) temp / range);
        if (index > bucketNum.length-1) {
            index = bucketNum.length-1;
            width = max - (range * (bucketNum.length - 1));
        }
        int left = temp - range * (index - 1);
        switch (op) {
            case EQUALS:
                return (width * sum) == 0 ? 0 : (double) bucketNum[index] / (width * sum);
            case LESS_THAN:
                double count = 0;
                for (int i = 0; i < index; i++) {
                    count += bucketNum[i];
                }
                count += (double) bucketNum[index] * (left-1) / width;
                return sum == 0 ? 0 : count/sum;
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
            case GREATER_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            default:
                throw new IllegalArgumentException("the op mismatch");
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        return sum == 0 ? 0.0 : (double) Arrays.stream(bucketNum).sum() / sum;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return "IntHistogram{" +
                "bucketNum=" + Arrays.toString(bucketNum) +
                ", sum=" + sum +
                ", range=" + range +
                ", min=" + min +
                ", max=" + max +
                '}';
    }
}
