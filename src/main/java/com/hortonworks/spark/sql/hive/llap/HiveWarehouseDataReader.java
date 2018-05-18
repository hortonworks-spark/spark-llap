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
import org.apache.hadoop.hive.llap.LlapArrowBatchRecordReader;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;

public class HiveWarehouseDataReader implements DataReader<ColumnarBatch> {

    private LlapArrowBatchRecordReader reader;
    private ArrowWrapperWritable wrapperWritable = new ArrowWrapperWritable();

    public HiveWarehouseDataReader(LlapInputSplit split, JobConf conf) throws Exception {
        LlapBaseInputFormat input = new LlapBaseInputFormat(true, Long.MAX_VALUE);
        this.reader = (LlapArrowBatchRecordReader) input.getRecordReader(split, conf, null);
    }

    @Override
    public boolean next() throws IOException {
        boolean hasNextBatch = reader.next(null, wrapperWritable);
        return hasNextBatch;
    }

    @Override
    public ColumnarBatch get() {
            List<FieldVector> fieldVectors = wrapperWritable.getVectorSchemaRoot().getFieldVectors();
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
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

}
