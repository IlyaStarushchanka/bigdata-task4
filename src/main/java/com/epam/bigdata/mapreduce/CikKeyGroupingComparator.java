package com.epam.bigdata.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Ilya_Starushchanka on 9/7/2016.
 */
public class CikKeyGroupingComparator extends WritableComparator {

    public CikKeyGroupingComparator(){
        super(CikWritable.class, true);
    }

    @Override
    public int compare (WritableComparable a, WritableComparable b){
        CikWritable cik1 = (CikWritable)a;
        CikWritable cik2 = (CikWritable)b;
        return cik1.getiPinyouID().compareToIgnoreCase(cik2.getiPinyouID());
    }

}
