package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;

    private TupleDesc td;
    private Map<Field, Tuple> groupBy;
    private Map<Field, Tuple> countSums;
    private Tuple noGrouping;
    private int noGroupCount;
    private int noSum;

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (this.gbfield == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            this.td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        }
        this.noGroupCount = 0;
        this.noSum = 0;
        this.groupBy = new HashMap<>();
        this.countSums = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field agg = tup.getField(this.afield);
        if (this.gbfield == NO_GROUPING) {
            this.noGroupCount += 1;
            if (this.noGrouping == null) {
                this.noGrouping = new Tuple(this.td);
                this.noGrouping.setField(0, agg);
                this.noSum = ((IntField) agg).getValue();
            } else {
                IntField curr = (IntField) this.noGrouping.getField(0);
                IntField place = helper(curr, (IntField) agg, null);
                this.noGrouping.setField(0, place);
            }
        } else {
            Field groupField = tup.getField(this.gbfield);
            if (this.groupBy.containsKey(groupField)) {
                Tuple countSum = this.countSums.get(groupField);
                int count = ((IntField) countSum.getField(0)).getValue();
                int sum = ((IntField) countSum.getField(1)).getValue();
                countSum.setField(0, new IntField(count + 1));
                countSum.setField(1, new IntField(sum + ((IntField) agg).getValue()));
                this.countSums.put(groupField, countSum);

                IntField curr = (IntField) this.groupBy.get(groupField).getField(1);
                IntField newValue = helper(curr, (IntField) agg, groupField);

                Tuple t = new Tuple(this.td);
                t.setField(0, groupField);
                t.setField(1, newValue);
                this.groupBy.put(groupField, t);
            } else {
                Tuple t = new Tuple(this.td);
                t.setField(0, groupField);
                t.setField(1, agg);

                this.groupBy.put(groupField, t);

                Tuple countSum = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE}));
                countSum.setField(0, new IntField(1));
                countSum.setField(1, agg);
                this.countSums.put(groupField, countSum);
            }
        }
    }

    private IntField helper(IntField f1, IntField f2, Field groupByField) {
        int val1 = f1.getValue();
        int val2 = f2.getValue();
        switch (this.what) {
            case MIN:
                return new IntField(Math.min(val1, val2));
            case MAX:
                return new IntField(Math.max(val1, val2));
            case AVG:
                if (groupByField == null) {
                    this.noSum += val2;
                    return new IntField(this.noSum / this.noGroupCount);
                } else {
                    return new IntField(((IntField) this.countSums.get(groupByField).getField(1)).getValue() /
                            ((IntField) this.countSums.get(groupByField).getField(0)).getValue());
                }
            case COUNT:
                return new IntField(((IntField) this.countSums.get(groupByField).getField(0)).getValue());
            case SUM:
                return new IntField(val1 + val2);
        }
        return null;
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
        return new IntegerIterator(this.gbfield);
    }

    public class IntegerIterator implements OpIterator {

        private boolean opened;
        private Tuple[] arr;
        private int idx;

        public IntegerIterator(int gbfield) {
            this.opened = false;
            this.idx = 0;
            if (gbfield == NO_GROUPING) {
                this.arr = new Tuple[1];
                this.arr[0] = noGrouping;
            } else {
                this.arr = groupBy.values().toArray(new Tuple[0]);
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.opened = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.opened) {
                return (this.idx < this.arr.length);
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (this.opened) {
                if (this.hasNext()) {
                    return this.arr[idx++];
                }
            }
            throw new NoSuchElementException();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (this.opened) {
                this.idx = 0;
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public TupleDesc getTupleDesc() {
            return this.arr[0].getTupleDesc();
        }

        @Override
        public void close() {
            this.opened = false;
        }
    }

}
