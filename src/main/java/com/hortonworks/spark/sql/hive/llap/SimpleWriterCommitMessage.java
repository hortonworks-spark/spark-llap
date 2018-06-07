package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

public class SimpleWriterCommitMessage implements WriterCommitMessage {
  private String message;

  public SimpleWriterCommitMessage(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
