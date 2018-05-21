package com.hortonworks.spark.sql.hive.llap;

import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class HiveWarehouseDataReaderFactory implements DataReaderFactory<ColumnarBatch> {
    private byte[] splitBytes;
    private byte[] confBytes;
    private transient InputSplit split;

    //No-arg constructor for executors
    public HiveWarehouseDataReaderFactory() {}

    //Driver-side setup
    public HiveWarehouseDataReaderFactory(InputSplit split, JobConf conf) {
        this.split = split;
        try {
            //Serialize split and conf for executors
            //TODO optimize/compress
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            split.write(dos);
            dos.close();
            splitBytes = baos.toByteArray();
            baos.reset();
            DataOutputStream dos2 = new DataOutputStream(baos);
            conf.write(dos2);
            dos2.close();
            confBytes = baos.toByteArray();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String[] preferredLocations() {
        try {
            return this.split.getLocations();
        } catch(Exception e) {
            //preferredLocations specifies to return empty array if no preference
            return new String[0];
        }
    }

    @Override
    public DataReader<ColumnarBatch> createDataReader() {
        LlapInputSplit llapInputSplit = new LlapInputSplit();
        ByteArrayInputStream bais = new ByteArrayInputStream(splitBytes);
        DataInputStream dis = new DataInputStream(bais);
        JobConf conf = new JobConf();
        ByteArrayInputStream bais2 = new ByteArrayInputStream(confBytes);
        DataInputStream dis2 = new DataInputStream(bais2);
        try {
            llapInputSplit.readFields(dis);
            conf.readFields(dis2);
            dis.close();
            dis2.close();
            return new HiveWarehouseDataReader(llapInputSplit, conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
