package com.mediatvcom.sfr.services.sql;

import com.beust.jcommander.Parameter;
import com.mediatvcom.sfr.services.sql.workflow.CatchupWorkflow;
import com.mediatvcom.sfr.services.sql.workflow.VodWorkflow;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.types.DataTypes.createStructField;

/**
 * Created by AGOUTA on 22\\08\\2016.
 */
public class SparkSqlRunner {

    @Parameter(names = "day", description = "convert all Csv files to parquet files on day 'day'", required = true)
    String day="2016-07-08";
    @Parameter(names = "--src-path", description = "the root path where Csv files are located", required = true)
    String rootCsv = "C:\\Users\\agouta\\Desktop\\output.tar\\output";
    @Parameter(names = "--content-type", description = "Might be either vod, catch-up or Sdv", required = true)
    String contentType = "catchup";


    public static void main(String[] args) {
        System.out.println("Working directory = " + System.getProperty("user.dir"));
        SparkSqlRunner app = new SparkSqlRunner();
        app.start();
    }

    private void start() {
        

        SparkSession spark = sparkSessionBuilder();

        if (contentType.equals("vod")){
            new VodWorkflow(spark, rootCsv, day).runWorkflow();
        }
        else if (contentType.equals("catchup")){
            new CatchupWorkflow(spark, rootCsv, day).runWorkflow();
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


}
