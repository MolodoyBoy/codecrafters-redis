package com.my.redis.data;

import static com.my.redis.data.DataType.*;

public class ArrayData implements Data {

    private final Data[] data;

    public ArrayData(int length) {
        if (length == -1) {
            data = null;
        } else {
            this.data = new Data[length];
        }
    }

    public void addData(int index, Data value) {
        if (data == null) {
            throw new IllegalStateException("Cannot add data to null array");
        }

        data[index] = value;
    }

    public Data[] getData() {
        return data;
    }

    public boolean isEmpty() {
        return data != null && data.length == 0;
    }

    @Override
    public boolean isNull() {
        return data == null;
    }

    @Override
    public DataType getType() {
        return ARRAY;
    }
}