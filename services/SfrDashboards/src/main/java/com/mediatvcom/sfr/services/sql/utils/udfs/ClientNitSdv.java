package com.mediatvcom.sfr.services.sql.utils.udfs;

import org.apache.spark.sql.api.java.UDF1;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by AGOUTA on 01/09/2016.
 */
public class ClientNitSdv implements UDF1<String, String> {
    private static final long serialVersionUID = -5372447039252716846L;

    Pattern patternClientId = Pattern.compile("(.+?)\\.(\\d+)([0-9]{3})([0-9]{2})");

    @Override
    public String call(String s) {
        try{
            Matcher m = patternClientId.matcher(s);
            if (m.find()){
                return m.group(2);
            }
        }
        catch (Exception e){
            return s;
        }
        return s;
    }
}