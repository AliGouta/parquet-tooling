package com.mediatvcom.sfr.services.sql.utils.udfs;

import org.apache.spark.sql.api.java.UDF1;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by AGOUTA on 01/09/2016.
 */
public class ErrorCategory implements UDF1<String, String> {
    private static final long serialVersionUID = -5372447039252716846L;

    Pattern patternError1 = Pattern.compile("((.+?)|^)Associated account i(.*)");
    Pattern patternError2 = Pattern.compile("((.+?)|^)Asset definition or(.*)");
    Pattern patternError3 = Pattern.compile("((.+?)|^)All configured strea(.*)");
    Pattern patternError4 = Pattern.compile("((.+?)|^)All Servers in the s(.*)");
    Pattern patternError5 = Pattern.compile("((.+?)|^)All suitable ERMs ar(.*)");
    Pattern patternError6 = Pattern.compile("((.+?)|^)Could not contact an(.*)");
    Pattern patternError7 = Pattern.compile("((.+?)|^)Endpoint: Device Erm(.*)");
    Pattern patternError8 = Pattern.compile("((.+?)|^)Endpoint: Device Scs(.*)");
    Pattern patternError9 = Pattern.compile("((.+?)|^)Invalid transport he(.*)");
    Pattern patternError10 = Pattern.compile("((.+?)|^)MongoDB error: Excep(.*)");
    Pattern patternError11 = Pattern.compile("((.+?)|^)MongoDB error: Prema(.*)");
    Pattern patternError12 = Pattern.compile("((.+?)|^)MongoDB error: Timeo(.*)");
    Pattern patternError13 = Pattern.compile("((.+?)|^)No endpoint found fo(.*)");
    Pattern patternError14 = Pattern.compile("((.+?)|^)No service group wit(.*)");
    Pattern patternError15 = Pattern.compile("((.+?)|^)No STB exists with t(.*)");
    Pattern patternError16 = Pattern.compile("((.+?)|^)Not Enough Bandwidth(.*)");
    Pattern patternError17 = Pattern.compile("((.+?)|^)Not Found(.*)");
    Pattern patternError18 = Pattern.compile("((.+?)|^)Session does not exi(.*)");
    Pattern patternError19 = Pattern.compile("((.+?)|^)SRM overloaded - ple(.*)");
    Pattern patternError20 = Pattern.compile("((.+?)|^)STB associated with(.*)");
    Pattern patternError21 = Pattern.compile("((.+?)|^)Too many active requ(.*)");
    Pattern patternError22 = Pattern.compile("((.+?)|^)Unexpected RuntimeEx(.*)");
    Pattern patternError23 = Pattern.compile("((.+?)|^)Unknown Error Descri(.*)");

    @Override
    public String call(String s) {
        try{

            if (s == null){
                return "null";
            }
            else if (patternError1.matcher(s).matches()){
                return "Associated account i";
            }
            else if (patternError2.matcher(s).matches()){
                return "Asset definition or";
            }
            else if (patternError3.matcher(s).matches()){
                return "All configured strea";
            }
            else if (patternError4.matcher(s).matches()){
                return "All Servers in the s";
            }
            else if (patternError5.matcher(s).matches()){
                return "All suitable ERMs ar";
            }
            else if (patternError6.matcher(s).matches()){
                return "Could not contact an";
            }
            else if (patternError7.matcher(s).matches()){
                return "Endpoint: Device Erm";
            }
            else if (patternError8.matcher(s).matches()){
                return "Endpoint: Device Scs";
            }
            else if (patternError9.matcher(s).matches()){
                return "Invalid transport he";
            }
            else if (patternError10.matcher(s).matches()){
                return "MongoDB error: Excep";
            }
            else if (patternError11.matcher(s).matches()){
                return "MongoDB error: Prema";
            }
            else if (patternError12.matcher(s).matches()){
                return "MongoDB error: Timeo";
            }
            else if (patternError13.matcher(s).matches()){
                return "No endpoint found fo";
            }
            else if (patternError14.matcher(s).matches()){
                return "No service group wit";
            }
            else if (patternError15.matcher(s).matches()){
                return "No STB exists with t";
            }
            else if (patternError16.matcher(s).matches()){
                return "Not Enough Bandwidth";
            }
            else if (patternError17.matcher(s).matches()){
                return "Not Found";
            }
            else if (patternError18.matcher(s).matches()){
                return "Session does not exi";
            }
            else if (patternError19.matcher(s).matches()){
                return "SRM overloaded - ple";
            }
            else if (patternError20.matcher(s).matches()){
                return "STB associated with";
            }
            else if (patternError21.matcher(s).matches()){
                return "Too many active requ";
            }
            else if (patternError22.matcher(s).matches()){
                return "Unexpected RuntimeEx";
            }
            else if (patternError23.matcher(s).matches()){
                return "Unknown Error Descri";
            }
            else{
                return "Other";
            }
        }
        catch (Exception e){
            return s;
        }
    }
}