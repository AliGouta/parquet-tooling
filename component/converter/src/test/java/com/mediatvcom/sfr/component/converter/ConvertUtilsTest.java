package com.mediatvcom.sfr.component.converter;

import org.junit.Test;

import static org.junit.Assert.*;

//import com.mediatvcom.sfr.component.converter.ConvertUtils.convertCsvToParquet
//import com.sun.java.util.jar.pack.Package;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

/**
 * Created by ali on 15/08/2016.
 */
public class ConvertUtilsTest {

    private static String output_file ="C:\\Users\\agouta\\IdeaProjects\\parquet-tooling\\component\\converter\\src\\main\\resources\\parquet\\output.parquet";
    private static String output_file_csv ="C:\\Users\\ali\\IdeaProjects\\parquet-tooling\\component\\converter\\src\\main\\resources\\tocsv\\output.csv";
    private static String input_file ="C:\\Users\\agouta\\IdeaProjects\\parquet-tooling\\component\\converter\\src\\main\\resources\\csv_sfr.csv";

    private final static File input_f = new File(input_file);
    private final static File output_f = new File(output_file);
    private final static File output_csv_f = new File(output_file_csv);

    @Test
    public void test_convert_to_parquet() {

        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        Locale.setDefault(new Locale("en", "US", "WIN"));
        run_parquet();

    }


    @Test
    public void test_convert_to_csv() {

        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        run_csv();

    }

    private void run_csv(){
        try {
            ConvertUtils.convertParquetToCSV(output_f, output_csv_f);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void run_parquet() {
        try {
            ConvertUtils.convertCsvToParquet(input_f,output_f);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}