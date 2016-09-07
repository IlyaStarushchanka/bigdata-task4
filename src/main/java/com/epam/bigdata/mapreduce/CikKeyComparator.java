package com.epam.bigdata.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Ilya_Starushchanka on 9/7/2016.
 */
public class CikKeyComparator extends WritableComparator {

    public CikKeyComparator(){
        super(CikWritable.class, true);
    }

    @Override
    public int compare (WritableComparable a, WritableComparable b){
        CikWritable cik1 = (CikWritable)a;
        CikWritable cik2 = (CikWritable)b;
        int cmp = cik1.getiPinyouID().compareToIgnoreCase(cik2.getiPinyouID());
        if (cmp != 0){
            return cmp;
        }
        return cik1.getTimestamp().compareTo(cik2.getTimestamp());
    }

}
