package com.mediatvcom.sfr.services.sql.utils.udfs;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by AGOUTA on 01/09/2016.
 */
public class Carte implements UDF1<String, String> {
    private static final long serialVersionUID = -5372447039252716846L;

    Pattern patternClientId = Pattern.compile("(.+?)\\.(.*)");

    @Override
    public String call(String s) {
        try{
            Matcher m = patternClientId.matcher(s);
            if (m.find()){
                return m.group(1);
            }
        }
        catch (Exception e){
            return s;
        }
        return s;
    }
}