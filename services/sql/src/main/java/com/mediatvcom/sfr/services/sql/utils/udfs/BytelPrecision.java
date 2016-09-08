package com.mediatvcom.sfr.services.sql.utils.udfs;

import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.execution.columnar.DOUBLE;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by AGOUTA on 01/09/2016.
 */
public class BytelPrecision implements UDF4<Long, String, Long, String, Double> {

    private static final long serialVersionUID = -5372447039252716846L;

    @Override
    public Double call(Long date_rep_seconds, String date_rep_micro, Long date_setup_seconds, String date_setup_micro) {
        return ( ( Double.valueOf(date_rep_micro) + ( Double.valueOf(date_rep_seconds) * 1000) ) - ( (Double.valueOf(date_setup_seconds) * 1000) + Double.valueOf(date_setup_micro) ) ) / 1000;
    }
}