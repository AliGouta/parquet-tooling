package com.mediatvcom.sfr.services.sql.utils.udfs;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by AGOUTA on 01/09/2016.
 */
public class CarteOn4vError implements UDF3<String, String, String, String> {

    Pattern patternClientIdOnSetup = Pattern.compile("(\\d+?)\\.(\\d+?)\\.(.*)");
    Pattern patternClientId = Pattern.compile("(.+?)\\.(.*)");
    Pattern pattern4vError = Pattern.compile("rtsp://localhost:8184(.*)");

    private static final long serialVersionUID = -5372447039252716846L;

    @Override
    public String call(String xsrmerror, String url, String clientid) {
        //TODO: Find better way to handle nullness of xsrmerror
        if (xsrmerror == null){
            return "";
        }
        else if (xsrmerror.equals("Missing client information")){
            return "unknown";
        }
        else{
            Matcher m1 = pattern4vError.matcher(url);
            if (m1.find()){
                Matcher m2 = patternClientIdOnSetup.matcher(clientid);
                if (m2.find()){
                    return m2.group(3);
                }
            }
            else{
                Matcher m4 = patternClientId.matcher(clientid);
                if (m4.find()){
                    return m4.group(1);
                }
            }
        }
        return "";
    }
}