package com.mediatvcom.sfr.services.sql.utils.udfs;

import org.apache.spark.sql.api.java.UDF1;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by AGOUTA on 01/09/2016.
 */
public class ContentNameOnSetup implements UDF1<String, String> {

    Pattern patternUrlToContentOnSetup = Pattern.compile("rtsp://localhost:8184/(.+?)(\\?definition.*|$)");
    private static final long serialVersionUID = -5372447039252716846L;

    @Override
    public String call(String s) {
        Matcher m = patternUrlToContentOnSetup.matcher(s);
        if (m.find()){
            return m.group(1);
        }
        return "";
    }
}