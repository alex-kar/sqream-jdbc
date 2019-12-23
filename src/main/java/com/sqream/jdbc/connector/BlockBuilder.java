package com.sqream.jdbc.connector;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.BitSet;

public class BlockBuilder {

    private int rowLength;
    private int blockSize;
    /**
     * Represents block as Object[ColumnIndex][RowIndex]
     */
    private Object[][] block;
    /**
     * Indexes of cells that already set in current row.
     */
    private BitSet curRowSet;
    /**
     * Point to the current row.
     */
    private int cursor;

    private ColumnsMetadata metadata;

    public BlockBuilder(int rowLength, int blockSize, ColumnsMetadata metadata) {
        this.rowLength = rowLength;
        this.blockSize = blockSize;
        this.metadata = metadata;
        init();
    }

    public void addValue(int index, Object value) throws ConnException {
        validateColumnIndex(index);
        validateCursor();
        try {
            block[index][cursor] = value;
        } catch (ArrayStoreException e) {
            throw new ConnException(String.format("Trying to set a value of type %s to column number %s of type %s",
                    value.getClass(), index, metadata.getType(index)));
        }
        curRowSet.set(index);
    }

    /**
     * Validate set values and move cursor to the next row.
     */
    public void buildRow() throws ConnException {
        if (isColumnsSet()) {
            cursor++;
            curRowSet.clear();
        } else {
            throw new ConnException(
                    String.format("All columns must be set before calling next(). Set %s columns out of %s",
                            curRowSet.cardinality(), rowLength));
        }
    }

    public BlockDto buildBlock() {
        BlockDto result = new BlockDto(block);
        init();
        return result;
    }

    private void init() {
        initStorage();
        this.curRowSet = new BitSet(rowLength);
        this.cursor = 0;
    }

    private void initStorage() {
        block = new Object[rowLength][];
        for (int i = 0; i < rowLength; i++) {
            block[i] = createArrayByType(metadata.getType(i), blockSize);
        }
    }

    /**
     * Checks that all columns in current row already set
     *
     * @return true  - all columns set
     *         false - one or more columns wasn't set
     */
    private boolean isColumnsSet() {
        return curRowSet.cardinality() == rowLength;
    }

    private void validateColumnIndex(int index) {
        if (index < 0 || index >= rowLength) {
            throw new IllegalArgumentException(
                    String.format("Wrong column index: [%s]. Should be from 0 to %s", index, rowLength - 1));
        }
    }

    private void validateCursor() throws ConnException {
        if (cursor >= blockSize) {
            throw new ConnException(String.format("Block is full. Size: %s", blockSize));
        }
    }

    private static Object[] createArrayByType(String type, int size) {
        if (type.equals("ftBool"))
            return new Boolean[size];
        else if (type.equals("ftUByte"))
            return new Byte[size];
        else if (type.equals("ftShort"))
            return new Short[size];
        else if (type.equals("ftInt"))
            return new Integer[size];
        else if (type.equals("ftLong"))
            return new Long[size];
        else if (type.equals("ftFloat"))
            return new Float[size];
        else if (type.equals("ftDouble"))
            return new Double[size];
        else if (type.equals("ftDate"))
            return new Date[size];
        else if (type.equals("ftDateTime"))
            return new Timestamp[size];
        else if (type.equals("ftVarchar"))
            return new String[size];
        else if (type.equals("ftBlob"))
            throw  new NotImplementedException();
        else {
            throw new UnsupportedOperationException(String.format("Column type [%s] is not supported", type));
        }
    }
}
