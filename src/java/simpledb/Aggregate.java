package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int afield, gfield;
    private Aggregator.Op aop;
    private Aggregator helper;
    private OpIterator itr;
    private TupleDesc ctd;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.ctd = this.child.getTupleDesc();

        if (this.gfield == -1) {
            if (this.ctd.getFieldType(this.afield) == Type.INT_TYPE) {
                this.helper = new IntegerAggregator(gfield, null, afield, aop);
            } else if (this.ctd.getFieldType(this.afield) == Type.STRING_TYPE) {
                this.helper = new StringAggregator(gfield, null, afield, aop);
            }
        } else {
            if (this.ctd.getFieldType(this.afield) == Type.INT_TYPE) {
                this.helper = new IntegerAggregator(gfield, this.ctd.getFieldType(this.gfield), afield, aop);
            } else if (this.ctd.getFieldType(this.afield) == Type.STRING_TYPE) {
                this.helper = new StringAggregator(gfield, this.ctd.getFieldType(this.gfield), afield, aop);
            }
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        if (this.gfield != -1) {
            return this.child.getTupleDesc().getFieldName(this.gfield);
        }
	    return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    return this.ctd.getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        this.child.open();
        while (this.child.hasNext()) {
            this.helper.mergeTupleIntoGroup(this.child.next());
        }
        this.itr = this.helper.iterator();
        this.itr.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.itr.hasNext()) {
            return this.itr.next();
        }
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    this.child.rewind();
	    this.itr.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        if (this.gfield == -1) {
            Type[] type = new Type[]{this.ctd.getFieldType(this.afield)};
            String[] name = new String[]{this.aggregateFieldName() + "(" + nameOfAggregatorOp(this.aop) + ") ("
                    + this.ctd.getFieldName(this.afield) + ")"};
            return new TupleDesc(type, name);
        } else {
            Type[] type = new Type[]{this.ctd.getFieldType(this.gfield), this.ctd.getFieldType(this.afield)};
            String[] name = new String[]{this.groupFieldName(), this.aggregateFieldName() + "(" + nameOfAggregatorOp(this.aop) + ") ("
                    + this.ctd.getFieldName(this.afield) + ")"};
            return new TupleDesc(type, name);
        }
    }

    public void close() {
        super.close();
        this.child.close();
        this.itr.close();
    }

    @Override
    public OpIterator[] getChildren() {
	    return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    this.child = children[0];
    }
    
}
