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
    public DataType getType() {
        return ARRAY;
    }

    @Override
    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append(Character.toString(getType().getValue()));

        if (data == null) {
            sb.append(new NullData().encode());

            return sb.toString();
        }

        sb.append(data.length);
        for (Data d : data) {
            sb.append(d.encode());
        }

        return sb.toString();
    }
}