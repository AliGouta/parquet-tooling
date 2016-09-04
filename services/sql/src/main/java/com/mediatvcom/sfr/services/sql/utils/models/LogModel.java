package com.mediatvcom.sfr.services.sql.utils.models;

import org.apache.spark.sql.types.StructType;

/**
 * Created by AGOUTA on 22/08/2016.
 */
public interface LogModel {

    public String getModelName();
    public StructType getSchema();
    public String getRootCsv();
    public String getLogComponent();

}
