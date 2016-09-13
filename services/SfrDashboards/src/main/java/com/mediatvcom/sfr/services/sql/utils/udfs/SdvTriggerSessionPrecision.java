package com.mediatvcom.sfr.services.sql.utils.udfs;

import org.apache.spark.sql.api.java.UDF2;

/**
 * Created by AGOUTA on 01/09/2016.
 */
public class SdvTriggerSessionPrecision implements UDF2<Long, String, Double> {

    private static final long serialVersionUID = -5372447039252716846L;

    @Override
    public Double call(Long date1, String date2) {
        return Double.valueOf(date1) * 1000 + Double.valueOf(date2);
    }
}