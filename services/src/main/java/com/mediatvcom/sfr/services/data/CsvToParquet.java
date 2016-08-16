package com.mediatvcom.sfr.services.data;

import com.beust.jcommander.Parameter;

import java.io.File;
import java.util.List;

/**
 * Created by ali on 16/08/2016.
 */


public class CsvToParquet {


    @Parameter(names = "day", description = "convert all Csv files to parquet files on day 'day'", required = true)
    String day;
    @Parameter(names = "--src-path", description = "the root path where Csv files are located", required = true)
    String rootCsv;
    @Parameter(names = "--dest-path", description = "the root path where parquet files will be written", required = true)
    String rootParquet;




    public static void main(String args[]){

        CsvToParquet csvtoparuqet = new CsvToParquet();

        //TODO: Add Accept conditions
        csvtoparuqet.run();

    }

    private void run() {
        List<File> lfiles = buildFiles(day, rootCsv, rootParquet);
    }

    private List<File> buildFiles(String day, String rootcsv, String rootparquet) {
        List<File> res = null;

        return res;

    }


}
