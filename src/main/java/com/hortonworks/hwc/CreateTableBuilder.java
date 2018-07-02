package com.hortonworks.hwc;

public interface CreateTableBuilder {

  CreateTableBuilder ifNotExists();

  CreateTableBuilder column(String name, String type);

  CreateTableBuilder partition(String name, String type);

  CreateTableBuilder prop(String key, String value);

  CreateTableBuilder clusterBy(long numBuckets, String ... columns);

  void create();

}
