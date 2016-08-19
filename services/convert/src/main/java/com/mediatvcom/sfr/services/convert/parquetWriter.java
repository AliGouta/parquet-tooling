package com.mediatvcom.sfr.services.convert;

import com.mediatvcom.sfr.component.converter.ConvertUtils;
import com.beust.jcommander.Parameter;

import java.io.File;
import java.util.List;


/**
 * Created by ali on 18/08/2016.
 */
public class parquetWriter {


    @Parameter(names = "day", description = "convert all Csv files to parquet files on day 'day'", required = true)
    String day;
    @Parameter(names = "--src-path", description = "the root path where Csv files are located", required = true)
    String rootCsv;
    @Parameter(names = "--dest-path", description = "the root path where parquet files will be written", required = true)
    String rootParquet;

    public static void main(String args[]){

        parquetWriter csvtoparuqet = new parquetWriter();

        //TODO: Add Accept conditions
        csvtoparuqet.run();

    }

    private void run() {
        List<File> lfiles = buildFiles(day, rootCsv, rootParquet);
        for (File file: lfiles) {
            ConvertUtils.convertParquetToCSV(file);


        }
    }

    private List<File> buildFiles(String day, String rootcsv, String rootparquet) {
        List<File> res = null;
        res.add(new File(rootcsv));
        return res;
    }
}
