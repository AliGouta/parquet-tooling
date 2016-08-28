package com.mediatvcom.sfr.services.convert;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;

import static com.mediatvcom.sfr.services.convert.SchameUtils.*;

/**
 * Created by AGOUTA on 21/08/2016.
 */

@Getter
public class ParquetFileSupport {

    Pattern patternModel = Pattern.compile(".*usrm\\\\(.*)\\\\[0-9]{4}-[0-9]{2}-[0-9]{2}.*");

    private final String schema;
    private final String fileParquetName;

    public ParquetFileSupport(String rootcsv, String rootparquet, File file) {
        this.schema = extractSchema(file);
        this.fileParquetName = getParquetFileName(rootcsv, rootparquet, file);
    }

    private String getParquetFileName(String rootcsv, String rootparquet, File file) {
        int file_length = file.getAbsolutePath().length();

        String fileparquetname = rootparquet + file.getAbsolutePath().substring(rootcsv.length(),
                file_length - ".csv".length()) + ".parquet";

        return fileparquetname;
    }

    public String extractSchema(File file){
        Matcher m = patternModel.matcher(file.getAbsolutePath());
        if (m.find() && m.groupCount() == 1){
            return getSchemaUtil(m.group(1));
        }
        return null;
    }

    public String getSchemaUtil(String model){

        switch(model){
            case "usrm_vermserver_rx":
                return USRM_VERMSERVER_RX_SCHEMA;
            case "usrm_vermserver_tx":
                return USRM_VERMSERVER_TX_SCHEMA;
            case "usrm_snmp":
                return USRM_SNMP_SCHEMA;

            default:
                return null;
        }

    }

}
