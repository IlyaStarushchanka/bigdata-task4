package com.epam.bigdata.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Ilya_Starushchanka on 9/7/2016.
 */
public class CikWritable implements Writable , WritableComparable<CikWritable>{

    private String iPinyouID;
    private Long timestamp;

    public CikWritable(){

    }

    public CikWritable(String iPinyouID, Long timestamp){
        this.iPinyouID = iPinyouID;
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(CikWritable o) {
        int cmp = iPinyouID.compareToIgnoreCase(o.iPinyouID);
        if (cmp != 0){
            return cmp;
        }
        return timestamp.compareTo(o.timestamp);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(iPinyouID);
        dataOutput.writeLong(timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        iPinyouID = dataInput.readUTF();
        timestamp = dataInput.readLong();
    }

    public String getiPinyouID() {
        return iPinyouID;
    }

    public void setiPinyouID(String iPinyouID) {
        this.iPinyouID = iPinyouID;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
