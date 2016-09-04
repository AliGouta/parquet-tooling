package com.mediatvcom.sfr.services.sql;

import com.beust.jcommander.Parameter;
import com.mediatvcom.sfr.services.sql.workflow.BwErrors;
import com.mediatvcom.sfr.services.sql.workflow.CatchupWorkflow;
import com.mediatvcom.sfr.services.sql.workflow.NumericableVoDCatchup;
import com.mediatvcom.sfr.services.sql.workflow.VodWorkflow;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.createStructField;

/**
 * Created by AGOUTA on 22\\08\\2016.
 */

public class SparkSqlRunner {

    @Parameter(names = "--day-from", description = "convert all Csv files to parquet files on day 'day'", required = true)
    String dayFrom="2016-07-08";
    @Parameter(names = "--day-to", description = "convert all Csv files to parquet files on day 'day'", required = true)
    String dayTo="2016-07-09";
    @Parameter(names = "--src-path", description = "the root path where Csv files are located", required = true)
    String rootCsv = "C:\\Users\\agouta\\Desktop\\output.tar\\output";
    @Parameter(names = "--content-type", description = "Might be either vod, catch-up or Sdv", required = true)
    String contentType = "vodcatchupall";

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    //DateTimeFormatter dtfOut = DateTimeFormat.forPattern("MM/dd/yyyy");

    public static void main(String[] args) {
        System.out.println("Working directory = " + System.getProperty("user.dir"));
        SparkSqlRunner app = new SparkSqlRunner();
        app.start();
    }

    private void start() {
        

        SparkSession spark = sparkSessionBuilder();

        List<String> dateRange = getDateRange(dayFrom, dayTo);

        if (contentType.equals("vod")){
            new VodWorkflow(spark, rootCsv, dateRange).runWorkflow();
        }
        else if (contentType.equals("catchup")){
            new CatchupWorkflow(spark, rootCsv, dateRange).runWorkflow();
        }
        else if (contentType.equals("vodcatchupall")){
            new NumericableVoDCatchup(spark, rootCsv, dateRange).runWorkflow();
        }
        else if (contentType.equals("bw-errors")){
            new BwErrors(spark, rootCsv, dateRange).runWorkflow();
        }
        else if (contentType.equals("sdv")){
            //new SdvWorkflow(spark, rootCsv, day);
        }

        //df.show();
    }

    private SparkSession sparkSessionBuilder() {
        return SparkSession.builder()
                    .appName("CSV to Dataset")
                    .master("local")
                    .config("spark.executor.memory", "2g")
                    .config("spark.driver.memory", "2g")
                    .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
                    .getOrCreate();
    }

    public List<String> getDateRange(String dayfrom, String dayto){

        List<String> dateRange = new ArrayList<>();
        //List<Date> dates = new ArrayList<Date>();
        DateTime startDate = formatter.parseDateTime(dayFrom);
        DateTime endDate = formatter.parseDateTime(dayTo);

        int days = Days.daysBetween(startDate, endDate).getDays();

        if (days <= 0){
            throw new IllegalArgumentException("The argument --day-to should be equal or superior to (--day-to +1 ) ");
        }
        else{

            for (int i=0; i < days; i++) {
                DateTime d = startDate.withFieldAdded(DurationFieldType.days(), i);
                dateRange.add(formatter.print(d));
            }

        }

        return dateRange;

    }


}
