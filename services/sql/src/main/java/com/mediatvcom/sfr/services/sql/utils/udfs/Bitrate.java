package com.mediatvcom.sfr.services.sql.utils.udfs;

import org.apache.spark.sql.api.java.UDF1;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by AGOUTA on 01/09/2016.
 */
public class Bitrate implements UDF1<String, Long> {
    private static final long serialVersionUID = -5372447039252716846L;

    @Override
    public Long call(String s) {
        return Long.valueOf(s);
    }
}