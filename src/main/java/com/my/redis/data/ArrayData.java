package com.my.redis.data;

import static com.my.redis.Delimiter.*;
import static com.my.redis.data.DataType.*;

public class ArrayData implements Data {

    private int position;
    private final Data[] data;

    public ArrayData(int length) {
        if (length == -1) {
            data = null;
        } else {
            this.data = new Data[length];
        }
    }

    public ArrayData(Data[] data) {
       this.data = data;
       this.position = data.length;
    }

    public void addData(Data value) {
        if (data == null) {
            throw new IllegalStateException("Cannot add data to null array");
        }

        if (position == data.length) {
            throw new IndexOutOfBoundsException("ArrayData is full");
        }

        data[position++] = value;
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

        sb.append(position);
        sb.append(CRLF);

        for (Data d : data) {
            sb.append(d.encode());
        }

        return sb.toString();
    }
}