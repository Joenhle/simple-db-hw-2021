package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private TupleDesc tupleDesc;
    private RecordId recordId;
    private ArrayList<Field> fields;
    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if (td.numFields() <= 0) {
            System.err.printf("TupleDesc至少包含一个Filed, FiledNum=%d", td.numFields());
            System.exit(1);
        }
        tupleDesc = td;
        fields = new ArrayList<>();
        Iterator<TupleDesc.TDItem> iter = td.iterator();
        while (iter.hasNext()) {
            TupleDesc.TDItem next = iter.next();
            switch (next.fieldType) {
                case INT_TYPE:
                    fields.add(new IntField(0));
                    break;
                case STRING_TYPE:
                    //todo maxsize该怎么设置
                    fields.add(new StringField("", 200));
                    break;
                default:
                    System.err.printf("没有对应的fieldType");
                    System.exit(1);
            }
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if (i >= fields.size()) {
            System.err.printf("i超过fields大小, fields大小为%d", fields.size());
            System.exit(1);
        }
        fields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    @Override
    public String toString() {
        return "Tuple{" +
                "tupleDesc=" + tupleDesc +
                ", recordId=" + recordId +
                ", fields=" + fields +
                '}';
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        tupleDesc = td;
    }
}
