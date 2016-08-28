package com.mediatvcom.sfr.services.convert;

import com.mediatvcom.sfr.component.converter.ConvertUtils;
import com.beust.jcommander.Parameter;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by ali on 18/08/2016.
 */
public class parquetWriter {


    @Parameter(names = "day", description = "convert all Csv files to parquet files on day 'day'", required = true)
    String day="2016-07-08";

    @Parameter(names = "--src-path", description = "the root path where Csv files are located", required = true)
    String rootCsv = "C:\\Users\\agouta\\Desktop\\output.tar\\output";

    @Parameter(names = "--dest-path", description = "the root path where parquet files will be written", required = true)
    String rootParquet = "C:\\Users\\agouta\\Desktop\\output.tar\\output_parquet";

    Pattern pattern = Pattern.compile("(.+?)" + day + "(.*)");

    public static void main(String args[]){

        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        parquetWriter csvtoparuqet = new parquetWriter();

        //TODO: Add Accept conditions
        csvtoparuqet.run();

    }

    private void run() {
        List<File> lfiles_in = buildFiles(day, rootCsv, rootParquet);
        for (File file: lfiles_in) {
            ParquetFileSupport parquetfile = new ParquetFileSupport(rootCsv, rootParquet, file);
            convertToParquet(file, parquetfile.getFileParquetName(), parquetfile.getSchema());
        }
    }

    private void convertToParquet(File file, String fileParquetName, String schema) {
        try {
            ConvertUtils.convertCsvToParquet(file, new File(fileParquetName), schema);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<File> buildFiles(String day, String rootcsv, String rootparquet) {
        List<File> res = new ArrayList<File>();
        try {
            Files.find(Paths.get(rootcsv),
                    5,
                    (filePath, fileAttr) -> fileAttr.isRegularFile())
                    .filter(path -> compare(path.toAbsolutePath().toString()))
                    .forEach(path -> res.add(new File(path.toAbsolutePath().toString())));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    private boolean compare(String path) {
        Matcher m = pattern.matcher(path);
        return m.matches();
    }



}
