package simpledb;

import java.util.*;
/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] histogramBuckets;
    private int width;
    private int numTup;
    private int max, min;
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
        histogramBuckets = new int[buckets];
        width = (int) (Math.ceil((double) (max - min) / buckets));
        numTup = 0;
        this.max = max;
        this.min = min;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int location = Math.min((v - min) / width, histogramBuckets.length - 1);
    	this.histogramBuckets[location] += 1;
    	numTup += 1;
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
        double result = 0.0;
        int num, location;
    	switch (op) {
            case EQUALS:
                if (v < min || v > max) {
                    result = 0;
                    break;
                }
                num = this.histogramBuckets[Math.min((v - min) / width, histogramBuckets.length - 1)];
                result = (double) (num / width) / numTup;
                break;
            case GREATER_THAN:
                location = Math.min((v - min) / width, histogramBuckets.length - 1);
                if (v <= min) {
                    result = 1;
                    break;
                } else if (v >= max) {
                    result = 0;
                    break;
                }
                double bf = ((double) this.histogramBuckets[location] / numTup);
                int bRight = (location + 1) * width;
                double bPart = ((double) (bRight - v) / width);
                result = bf * bPart;
                for (int i = location + 1; i < this.histogramBuckets.length; i++) {
                    result += ((double) this.histogramBuckets[i] / numTup);
                }
                break;
            case LESS_THAN:
                location = Math.min((v - min) / width, histogramBuckets.length - 1);
                if (v <= min) {
                    result = 0;
                    break;
                } else if (v >= max) {
                    result = 1;
                    break;
                }
                double fraction = ((double) this.histogramBuckets[location] / numTup);
                int bLeft = location * width;
                double part = ((double) (v - bLeft) / width);
                result = fraction * part;
                for (int i = location - 1; i >= 0; i--) {
                    result += ((double) this.histogramBuckets[i] / numTup);
                }
                break;
            case LESS_THAN_OR_EQ:
                if (v <= min) {
                    result = 0;
                    break;
                } else if (v >= max) {
                    result = 1;
                    break;
                }
                double lessThan = estimateSelectivity(Predicate.Op.LESS_THAN, v);
                double equalsLess = estimateSelectivity(Predicate.Op.EQUALS, v);
                result = lessThan + equalsLess;
                break;
            case GREATER_THAN_OR_EQ:
                if (v <= min) {
                    result = 1;
                    break;
                } else if (v >= max) {
                    result = 0;
                    break;
                }
                double greaterThan = estimateSelectivity(Predicate.Op.GREATER_THAN, v);
                double equalsGreater = estimateSelectivity(Predicate.Op.EQUALS, v);
                result = greaterThan + equalsGreater;
                break;
            case NOT_EQUALS:
                result = 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
        }
        return result;
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
        List<Predicate.Op> opList = Arrays.asList(
                Predicate.Op.EQUALS,
                Predicate.Op.GREATER_THAN,
                Predicate.Op.LESS_THAN,
                Predicate.Op.LESS_THAN_OR_EQ,
                Predicate.Op.GREATER_THAN_OR_EQ,
                Predicate.Op.NOT_EQUALS);
        double sum = 0;

        return sum / opList.size();
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        for (int i = 0; i < this.histogramBuckets.length; i++) {
            System.out.println("Bucket " + i + " has a height of " + this.histogramBuckets[i]);
        }
        return null;
    }
}
