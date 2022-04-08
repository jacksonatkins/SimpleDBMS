package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private int numFields;
    private TDItem[] items;
    private int size;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {

        return new Iterator<>() {

            private int idx = 0;

            @Override
            public boolean hasNext() {
                return idx < numFields;
            }

            @Override
            public TDItem next() {
                return items[idx++];
            }
        };
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        assert typeAr.length > 0;
        this.size = 0;

        this.numFields = typeAr.length;
        this.items = new TDItem[this.numFields];
        for (int i = 0; i < this.numFields; i++) {
            this.size += typeAr[i].getLen();
            TDItem newItem = new TDItem(typeAr[i], fieldAr[i]);
            this.items[i] = newItem;
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        assert typeAr.length > 0;
        this.size = 0;

        this.numFields = typeAr.length;
        this.items = new TDItem[this.numFields];
        for (int i = 0; i < this.numFields; i++) {
            this.size += typeAr[i].getLen();
            TDItem newItem = new TDItem(typeAr[i], null);
            this.items[i] = newItem;
        }
    }


    /**
     * Constructor. Create a new tuple desc from the given array of fields and names.
     *
     * @param newItems array specifying the number of and types of fields to be used, as well
     *              as their corresponding names
     */
    public TupleDesc(TDItem[] newItems) {
        this.numFields = newItems.length;
        this.items = newItems;
        this.size = 0;
        for (TDItem t : newItems) {
            this.size += t.fieldType.getLen();
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i > this.numFields) {
            throw new NoSuchElementException("Not a valid field reference.");
        }
        return this.items[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i > this.numFields) {
            throw new NoSuchElementException("Not a valid field reference.");
        }
        return this.items[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        int idx = 0;
        while (idx < this.numFields) {
            if (this.items[idx].fieldName == null) {
                idx++;
            } else if (this.items[idx].fieldName.equals(name)) {
                return idx;
            } else {
                idx++;
            }
        }
        throw new NoSuchElementException("Field with matching name not found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return this.size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        TDItem[] newitems = new TDItem[td1.numFields() + td2.numFields()];

        System.arraycopy(td1.items, 0, newitems, 0, td1.numFields());
        System.arraycopy(td2.items, 0, newitems, td1.numFields(), td2.numFields());

        return new TupleDesc(newitems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o instanceof TupleDesc) {
            TupleDesc td = (TupleDesc) o;
            if (td.numFields() != this.numFields) {
                return false;
            } else {
                for (int i = 0; i < this.numFields; i++) {
                    if (td.getFieldType(i) != this.getFieldType(i)) {
                        return false;
                    }
                }
                return true;
            }
        } else {
            return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return Arrays.hashCode(this.items);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String result = this.items[0].toString();
        for (int i = 1; i < this.numFields; i++) {
            result += ", " + this.items[i].toString();
        }
        return result;
    }
}
