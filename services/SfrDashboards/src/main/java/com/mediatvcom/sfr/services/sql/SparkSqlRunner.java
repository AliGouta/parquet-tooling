package com.mediatvcom.sfr.services.sql;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.mediatvcom.sfr.services.sql.workflow.*;
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

    @Parameter(names = "--day-from", description = "convert all Csv files to parquet files on day 'day'", required = false)
    String dayFrom="2016-07-08";
    @Parameter(names = "--day-to", description = "convert all Csv files to parquet files on day 'day'", required = false)
    String dayTo="2016-07-09";
    @Parameter(names = "--root-path", description = "the root path where Csv files are located", required = false)
    String rootCsv = "C:\\Users\\agouta\\Desktop\\output.tar\\output";

    @Parameter(names = "--dashboard-type", description = "Might be either vod, catch-up or Sdv", required = false)
    String contentType = "vodcatchupall";

    @Parameter(names = "--es-node", description = "Elasticsearch node, may take either the FQDN or IP adress", required = false)
    String esnode = "10.1.1.157";
    @Parameter(names = "--es-port", description = "Elasticsearch port", required = false)
    String esport = "9200";

    @Parameter(names = "--vod-catchup-all-index-type", description = "Index/type of Vod and catchup all", required = false)
    String index_vod_catchup_all = "vodcatchup/success_errors";

    @Parameter(names = "--canal-index-type", description = "Index/type of Canal bandwidth estimation", required = false)
    String index_type_canal = "canal/bw";

    @Parameter(names = "--bw-errors-index-type", description = "Index/type of bandwidth errors", required = false)
    String index_type_bw_errors = "bw_errors/not_enough_bandwidth";

    @Parameter(names = "--bytel-index-type", description = "Index/type of bandwidth errors", required = false)
    String index_type_bytel = "bytel/success_and_errors";

    @Parameter(names = "--sdv-index-type", description = "Index/type of sdv sessions", required = false)
    String index_type_sdv = "sdv/all";

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");

    public static void main(String[] args) {

        //System.out.println("Working directory = " + System.getProperty("user.dir"));
        SparkSqlRunner app = new SparkSqlRunner();
        new JCommander(app, args);
        app.start();
    }

    private void start() {

        SparkSession spark = sparkSessionBuilder();

        List<String> dateRange = getDateRange();

        if (contentType.equals("vod")){
            new VodWorkflow(spark, rootCsv, dateRange).runWorkflow();
        }
        else if (contentType.equals("catchup")){
            new CatchupWorkflow(spark, rootCsv, dateRange).runWorkflow();
        }
        else if (contentType.equals("vodcatchupall")){
            new NumericableVoDCatchup(spark, rootCsv, dateRange, esnode, esport, index_vod_catchup_all, index_type_canal).runWorkflow();
        }
        else if (contentType.equals("bw-errors")){
            new BwErrors(spark, rootCsv, dateRange, esnode, esport, index_type_bw_errors).runWorkflow();
        }
        else if (contentType.equals("bytel")){
            new Bytel(spark, rootCsv, dateRange, esnode, esport, index_type_bytel).runWorkflow();
        }
        else if (contentType.equals("sdv")){
            new SDV(spark, rootCsv, dateRange, esnode, esport, index_type_sdv).runWorkflow();
        }

        spark.stop();

    }

    private SparkSession sparkSessionBuilder() {
        return SparkSession.builder()
                    //.master("local[*]")
                    //.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
                    .appName("SFR Dashboards")
                    .getOrCreate();
    }

    public List<String> getDateRange(){

        List<String> dateRange = new ArrayList<>();
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
