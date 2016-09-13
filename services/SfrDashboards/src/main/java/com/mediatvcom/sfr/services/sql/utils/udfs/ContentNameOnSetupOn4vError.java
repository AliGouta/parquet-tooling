package com.mediatvcom.sfr.services.sql.utils.udfs;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by AGOUTA on 01/09/2016.
 */
public class ContentNameOnSetupOn4vError implements UDF1<String, String> {
    private static final long serialVersionUID = -5372447039252716846L;

    Pattern patternUrlToContent4verror = Pattern.compile("rtsp://(.+?)/(?:\\?SDV=(.*)|(.+?)(?:\\?definition.*|$))");

    @Override
    public String call(String url) {
        if (url == null){
            return "";
        }
        else{
            Matcher m = patternUrlToContent4verror.matcher(url);
            if (m.find()){
                if (m.group(2) == null) {
                    return m.group(3);
                }
                return m.group(2);
            }
        }
        return "";
    }
}