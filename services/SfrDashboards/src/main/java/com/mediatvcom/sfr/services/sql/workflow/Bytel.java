package com.mediatvcom.sfr.services.sql.workflow;

import com.mediatvcom.sfr.services.sql.utils.models.srm.*;
import com.mediatvcom.sfr.services.sql.utils.udfs.*;
import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.callUDF;


/**
 * Created by AGOUTA on 22/08/2016.
 */
public class Bytel implements Serializable {

    private final SparkSession spark;
    List<String> dateRange;

    private final String esNode;
    private final String esPort;
    private final String indexBytel;

    /*
    Models used by the VoD Workflow
     */
    private final SrmSetup1bytelModel srmSetup1bytelModel;
    private final SrmResponse1bytelModel srmResponse1bytelModel;



    /*
    DataFrames for VoD
     */
    Dataset<Row> df_srmSetup1bytelModel;
    Dataset<Row> df_srmResponse1bytelModel;


    public Bytel(SparkSession spark, String rootCsv, List<String> daterange, String esnode, String esport, String indexbytel) {
        this.spark = spark;
        this.dateRange = daterange;

        this.esNode = esnode;
        this.esPort = esport;
        this.indexBytel = indexbytel;

        this.srmSetup1bytelModel = new SrmSetup1bytelModel(rootCsv);
        this.srmResponse1bytelModel = new SrmResponse1bytelModel(rootCsv);
        this.df_srmSetup1bytelModel = getDframe(srmSetup1bytelModel.getRootCsv(), srmSetup1bytelModel.getLogComponent(), srmSetup1bytelModel.getModelName(), srmSetup1bytelModel.getSchema(), dateRange);
        this.df_srmResponse1bytelModel = getDframe(srmResponse1bytelModel.getRootCsv(), srmResponse1bytelModel.getLogComponent(), srmResponse1bytelModel.getModelName(), srmResponse1bytelModel.getSchema(), dateRange);

    }

     public void  runWorkflow(){

         df_srmSetup1bytelModel.createOrReplaceTempView("srmSetup1bytelModel");
         df_srmResponse1bytelModel.createOrReplaceTempView("srmResponse1bytelModel");

         spark.udf().register("getTimePrecision", new BytelPrecision(), DataTypes.DoubleType);



         //["date", "url", "cseq", "content_type", "ott_component"]

         Dataset<Row> sqlDF = spark.sql("SELECT 1bytelsetup.date as date, 1bytelsetup.cseq as cseq, " +
                 "1bytelsetup.content_type as content_type, 1bytelresponse.code_http as code_http, 1bytelresponse.x_srm_error_message as x_srm_error_message, " +
                 "unix_timestamp(1bytelresponse.date, \"yyyy-MM-dd HH:mm:ss.SSS\") as date_rep_seconds, " +
                 "regexp_extract(1bytelresponse.date,\".+?(.)([0-9]{3})\",2) as date_rep_micro, " +
                 "unix_timestamp(1bytelsetup.date, \"yyyy-MM-dd HH:mm:ss.SSS\") as date_setup_seconds, " +
                 "regexp_extract(1bytelsetup.date,\".+?(.)([0-9]{3})\",2) as date_setup_micro " +
                 "From ( " +
                    "SELECT trsetup.date as date, trsetup.cseq as cseq, trsetup.content_type as content_type " +
                    "FROM  srmSetup1bytelModel trsetup " +
                    "WHERE to_date(trsetup.date) = to_date( " + "\"" + dateRange.get(0) + "\"" + " ) " +
                 ") 1bytelsetup " +
                 "JOIN srmResponse1bytelModel 1bytelresponse " +
                 "ON 1bytelresponse.cseq = 1bytelsetup.cseq " +
                 "WHERE ( unix_timestamp(1bytelresponse.date, \"yyyy-MM-dd HH:mm:ss.SSS\") - unix_timestamp(1bytelsetup.date, \"yyyy-MM-dd HH:mm:ss.SSS\") ) < 120 and " +
                 "( unix_timestamp(1bytelresponse.date, \"yyyy-MM-dd HH:mm:ss.SSS\") + 120 ) > unix_timestamp(1bytelsetup.date, \"yyyy-MM-dd HH:mm:ss.SSS\")  ");


         sqlDF = sqlDF.withColumn("time_response", callUDF("getTimePrecision", sqlDF.col("date_rep_seconds"), sqlDF.col("date_rep_micro"), sqlDF.col("date_setup_seconds"), sqlDF.col("date_setup_micro")));
         sqlDF.createOrReplaceTempView("bytel");

         Dataset<Row> sqlBytel = spark.sql(" SELECT from_utc_timestamp(from_unixtime(unix_timestamp(date, \"yyyy-MM-dd HH:mm:ss.SSS\")), 'Europe/Paris') as date, " +
                 "cseq, content_type, code_http, x_srm_error_message, time_response " +
                 "FROM bytel ").cache();

         sqlBytel.createOrReplaceTempView("bytel_final");

         Dataset<Row> sqlFinalBytel = spark.sql("SELECT " +
                 "date, " +
                 "if (cseq IS NOT NULL, cseq, \"null\") as cseq, " +
                 "if (content_type IS NOT NULL, content_type, \"null\") as content_type, " +
                 "if (code_http IS NOT NULL, code_http, \"null\") as code_http, " +
                 "if (x_srm_error_message IS NOT NULL, x_srm_error_message, \"null\") as x_srm_error_message, " +
                 "if (time_response IS NOT NULL, time_response, 0) as time_response " +
                 "FROM bytel_final ").repartition(1);



         Map<String, String> cfg = new HashedMap();
         cfg.put("es.nodes", esNode);
         cfg.put("es.port", esPort);
         cfg.put("es.resource", indexBytel);
         cfg.put("es.spark.dataframe.write.null", "true");

         JavaEsSparkSQL.saveToEs(sqlFinalBytel, cfg);

         /*
        root
         |-- date: timestamp (nullable = true)
         |-- cseq: string (nullable = true)
         |-- content_type: string (nullable = true)
         |-- code_http: string (nullable = true)
         |-- x_srm_error_message: string (nullable = true)
         |-- time_response: double (nullable = true)
          */

         sqlFinalBytel.printSchema();

         /*

         sqlFinalBytel.show();
         sqlFinalBytel.write().csv("C:\\temp\\result-utc-bytel.csv");
*/


     }

    private Dataset<Row> getDframe(String rootcsv, String logcomponent, String model, StructType schema, List<String> daterange) {

        String[] files = getFilePaths(rootcsv, logcomponent, model, daterange);

        return spark.read()
                .format("csv")
                .option("header", "false")
                .option("delimiter", ";")
                .option("nullValue", "\"\"")
                .schema(schema)
                .csv(files);

    }

    private String[] getFilePaths(String rootcsv, String logcomponent, String model, List<String> daterange) {
        int ldays = daterange.size();
        String[] files = new String[ldays];
        int i=0;
        for (String day : daterange){
            //files[i] = rootcsv + "\\output_logstash\\"+ logcomponent +"\\"+ model + "\\" + day + "\\data.csv";
            files[i] = rootcsv + "/output_logstash/"+ logcomponent +"/"+ model + "/" + day + "/data.csv";
            i++;
        }
        return files;
    }
}
