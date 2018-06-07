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
    private long arrowAllocatorMax;

    //No-arg constructor for executors
    public HiveWarehouseDataReaderFactory() {}

    //Driver-side setup
    public HiveWarehouseDataReaderFactory(InputSplit split, JobConf conf, long arrowAllocatorMax) {
        this.split = split;
        this.arrowAllocatorMax = arrowAllocatorMax;
        ByteArrayOutputStream splitByteArrayStream = new ByteArrayOutputStream();
        ByteArrayOutputStream confByteArrayStream = new ByteArrayOutputStream();

        try(DataOutputStream splitByteData = new DataOutputStream(splitByteArrayStream);
            DataOutputStream confByteData = new DataOutputStream(confByteArrayStream)) {
            //Serialize split and conf for executors
            split.write(splitByteData);
            splitBytes = splitByteArrayStream.toByteArray();
            conf.write(confByteData);
            confBytes = confByteArrayStream.toByteArray();
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
        ByteArrayInputStream splitByteArrayStream = new ByteArrayInputStream(splitBytes);
        ByteArrayInputStream confByteArrayStream = new ByteArrayInputStream(confBytes);
        JobConf conf = new JobConf();

        try(DataInputStream splitByteData = new DataInputStream(splitByteArrayStream);
            DataInputStream confByteData = new DataInputStream(confByteArrayStream)) {
            llapInputSplit.readFields(splitByteData);
            conf.readFields(confByteData);
            return getDataReader(llapInputSplit, conf, arrowAllocatorMax);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected DataReader<ColumnarBatch> getDataReader(LlapInputSplit split, JobConf jobConf, long arrowAllocatorMax)
        throws Exception {
        return new HiveWarehouseDataReader(split, jobConf, arrowAllocatorMax);
    }
}
