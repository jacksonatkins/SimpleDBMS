package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield, afield;
    private Type gbfieldtype;
    private Tuple noGrouping;
    private Map<Field, Integer> groupBy;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.groupBy = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (this.gbfield == NO_GROUPING) {
           if (this.noGrouping == null) {
               this.noGrouping = tup;
           } else {
               IntField res = new IntField(((IntField) this.noGrouping.getField(this.afield)).getValue() +
                       ((IntField) tup.getField(this.afield)).getValue());
               this.noGrouping.setField(this.afield, res);
           }
        } else {
            if (this.groupBy.containsKey(tup.getField(this.gbfield))) {
                int current = this.groupBy.get(tup.getField(this.gbfield));
                this.groupBy.put(tup.getField(this.gbfield), current + 1);
            } else {
                this.groupBy.put(tup.getField(this.gbfield), 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new StringIterator(this.gbfield);
    }

    public class StringIterator implements OpIterator {

        private boolean opened;
        private Tuple[] arr;
        private int idx;

        public StringIterator(int gbfield) {
            this.opened = false;
            this.idx = 0;
            if (gbfield == NO_GROUPING) {
                this.arr = new Tuple[1];
                this.arr[0]  = noGrouping;
            } else {
                int i = 0;
                this.arr = new Tuple[groupBy.size()];
                for (Field gbField : groupBy.keySet()) {
                    Tuple current = new Tuple(new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}));
                    current.setField(0, gbField);
                    current.setField(1, new IntField(groupBy.get(gbField)));
                    this.arr[i] = current;
                    i++;
                }
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
                    return this.arr[this.idx++];
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
