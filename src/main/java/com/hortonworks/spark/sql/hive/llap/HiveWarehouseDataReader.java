package com.hortonworks.spark.sql.hive.llap;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import java.util.Iterator;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hive.llap.LlapArrowInput;

public class HiveWarehouseDataReader implements DataReader<ColumnarBatch> {

    private LlapArrowInput reader;

    public HiveWarehouseDataReader(LlapInputSplit split, JobConf conf) throws Exception {
        LlapBaseInputFormat input = new LlapBaseInputFormat(true);
        this.reader = (LlapArrowInput) input.getRecordReader(split, conf, null);
    }

    @Override
    public boolean next() throws IOException {
        boolean hasNextBatch = reader.loadNextBatch();
        return hasNextBatch;
    }

    @Override
    public ColumnarBatch get() {
        try {
            List<FieldVector> fieldVectors = reader.getCurrentFieldVectors();
            ColumnVector[] columnVectors = new ColumnVector[fieldVectors.size()];
            Iterator<FieldVector> iterator = fieldVectors.iterator();
            int rowCount = -1;
            for(int i = 0; i < columnVectors.length; i++) {
                FieldVector fieldVector = iterator.next();
                columnVectors[i] = new ArrowColumnVector(fieldVector);
                if(rowCount == -1) {
                    rowCount = fieldVector.getValueCount();
                }
            }
            ColumnarBatch columnarBatch = new ColumnarBatch(columnVectors);
            columnarBatch.setNumRows(rowCount);
            return columnarBatch;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

}
