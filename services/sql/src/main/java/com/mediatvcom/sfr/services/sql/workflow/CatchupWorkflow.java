package com.mediatvcom.sfr.services.sql.workflow;

import com.mediatvcom.sfr.services.sql.utils.models.srm.*;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverRxModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverTxModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.callUDF;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by AGOUTA on 22/08/2016.
 */
public class CatchupWorkflow implements Serializable {

    private final SparkSession spark;
    private String rootCsv;
    private String day;

    /*
    Models used by the VoD Workflow
     */
    private final SrmGetContent0cModel srmGetContent0cModel;
    private final SrmPostContent1cModel srmPostContent1cModel;
    private final SrmRessource2v2cModel srmRessource2v2cModel;
    private final UsrmVermserverRxModel usrmVermserverRxModel;
    private final UsrmVermserverTxModel usrmVermserverTxModel;
    private final SrmSessionStart4cModel srmSessionStart4cModel;
    private final SrmSessionId3v5cModel srmSessionId3v5cModel;
    private final SrmTunningStartSession4v6v4s6cModel srmTunningStartSession4v6v4s6cModel;
    private final SrmEnd7v7cModel srmEnd7v7cModel;
    private final SrmEnd8cModel srmEnd8cModel;


    Pattern patternSessionId = Pattern.compile("Session: (.*);timeout=300");
    Pattern patternClientId = Pattern.compile("(.+?)\\.(.*)");

    /*
    DataFrames for Catchup
     */
    Dataset<Row> df_srmGetContent0cModel;
    Dataset<Row> df_srmPostContent1cModel;
    Dataset<Row> df_srmRessource2v2cModel;
    Dataset<Row> df_usrm_vermserver_rx;
    Dataset<Row> df_usrm_vermserver_tx;
    Dataset<Row> df_srmSessionStart4cModel;
    Dataset<Row> df_srmSessionId3v5cModel;
    Dataset<Row> df_srmTunningStartSession4v6v4s6cModel;
    Dataset<Row> df_srmEnd7v7cModel;
    Dataset<Row> df_srmEnd8cModel;


    public CatchupWorkflow(SparkSession spark, String rootCsv, String day) {
        this.spark = spark;
        this.rootCsv = rootCsv;
        this.day = day;

        this.srmGetContent0cModel = new SrmGetContent0cModel(day,rootCsv);
        this.srmPostContent1cModel = new SrmPostContent1cModel(day,rootCsv);
        this.srmRessource2v2cModel = new SrmRessource2v2cModel(day,rootCsv);
        this.usrmVermserverRxModel = new UsrmVermserverRxModel(day, rootCsv);
        this.usrmVermserverTxModel  = new UsrmVermserverTxModel(day, rootCsv);
        this.srmSessionStart4cModel = new SrmSessionStart4cModel(day, rootCsv);
        this.srmSessionId3v5cModel = new SrmSessionId3v5cModel(day,rootCsv);
        this.srmTunningStartSession4v6v4s6cModel = new SrmTunningStartSession4v6v4s6cModel(day, rootCsv);
        this.srmEnd7v7cModel  = new SrmEnd7v7cModel(day, rootCsv);
        this.srmEnd8cModel = new SrmEnd8cModel(day, rootCsv);

        this.df_srmGetContent0cModel = getDframe(srmGetContent0cModel.getRootCsv(), srmGetContent0cModel.getLogComponent(), srmGetContent0cModel.getModelName(), srmGetContent0cModel.getSchema(), day);
        this.df_srmPostContent1cModel = getDframe(srmPostContent1cModel.getRootCsv(), srmPostContent1cModel.getLogComponent(), srmPostContent1cModel.getModelName(), srmPostContent1cModel.getSchema(), day);
        this.df_srmRessource2v2cModel = getDframe(srmRessource2v2cModel.getRootCsv(), srmRessource2v2cModel.getLogComponent(), srmRessource2v2cModel.getModelName(),srmRessource2v2cModel.getSchema(), day);
        this.df_usrm_vermserver_rx = getDframe(usrmVermserverRxModel.getRootCsv(), usrmVermserverRxModel.getLogComponent(), usrmVermserverRxModel.getModelName(), usrmVermserverRxModel.getSchema(), day);
        this.df_usrm_vermserver_tx = getDframe(usrmVermserverTxModel.getRootCsv(), usrmVermserverTxModel.getLogComponent(), usrmVermserverTxModel.getModelName(), usrmVermserverTxModel.getSchema(), day);
        this.df_srmSessionStart4cModel = getDframe(srmSessionStart4cModel.getRootCsv(), srmSessionStart4cModel.getLogComponent(), srmSessionStart4cModel.getModelName(), srmSessionStart4cModel.getSchema(), day);
        this.df_srmSessionId3v5cModel = getDframe(srmSessionId3v5cModel.getRootCsv(), srmSessionId3v5cModel.getLogComponent(), srmSessionId3v5cModel.getModelName(), srmSessionId3v5cModel.getSchema(), day);
        this.df_srmTunningStartSession4v6v4s6cModel = getDframe(srmTunningStartSession4v6v4s6cModel.getRootCsv(), srmTunningStartSession4v6v4s6cModel.getLogComponent(), srmTunningStartSession4v6v4s6cModel.getModelName(), srmTunningStartSession4v6v4s6cModel.getSchema(), day);
        this.df_srmEnd7v7cModel = getDframe(srmEnd7v7cModel.getRootCsv(), srmEnd7v7cModel.getLogComponent(), srmEnd7v7cModel.getModelName(), srmEnd7v7cModel.getSchema(), day);
        this.df_srmEnd8cModel = getDframe(srmEnd8cModel.getRootCsv(), srmEnd8cModel.getLogComponent(), srmEnd8cModel.getModelName(), srmEnd8cModel.getSchema(), day);

    }

     public void  runWorkflow(){

         df_srmGetContent0cModel.createOrReplaceTempView("srmGetContent0cModel");
         df_srmPostContent1cModel.createOrReplaceTempView("srmPostContent1cModel");
         df_srmRessource2v2cModel.createOrReplaceTempView("srmRessource2v2cModel");
         df_usrm_vermserver_rx.createOrReplaceTempView("usrm_vermserver_rx");
         df_usrm_vermserver_tx.createOrReplaceTempView("usrm_vermserver_tx");
         /*
         df_srmSessionStart4cModel.createOrReplaceTempView("srmSessionStart4cModel");
         df_srmSessionId3v5cModel.createOrReplaceTempView("srmSessionId3v5cModel");
         df_srmTunningStartSession4v6v4s6cModel.createOrReplaceTempView("srmTunningStartSession4v6v4s6cModel");
         df_srmEnd7v7cModel.createOrReplaceTempView("srmEnd7v7cModel");
         */
         df_srmEnd8cModel.createOrReplaceTempView("srmEnd8cModel");

         spark.udf().register("getCarte", new UDF1<String, String>() {
             private static final long serialVersionUID = -5372447039252716846L;

             @Override
             public String call(String s) {
                 Matcher m = patternClientId.matcher(s);
                 if (m.find()){
                     return m.group(1);
                 }
                 return null;
             }
         }, DataTypes.StringType);

         df_srmSessionStart4cModel = df_srmSessionStart4cModel.withColumn("carteId", callUDF("getCarte", df_srmSessionStart4cModel.col("client_id")));
         df_srmSessionStart4cModel.createOrReplaceTempView("srmSessionStart4cModel");
         //df_srmSessionStart4cModel.show();

/*
         Dataset<Row> sqlDF = spark.sql(" " +
                 "SELECT date as date_4c, vip as vip_4c, content_name as content_name_4c, " +
                 "control_session as control_session_4c, content_type as content_type_4c, " +
                 "client_id as client_id_4c, carteId as carteId_4c " +
                 "From srmSessionStart4cModel 4c ").repartition(1);
*/

         Dataset<Row> sqlDF = spark.sql("SELECT rx.date as date_rx, 2v.date as date_2v, 2v.ondemand_session_id as ondemand_session_id_2v, " +
                 "2v.code_http as code_http_2v, rx.bitrate as bitrate_rx, rx.service_group as service_group_rx, rx.ip_rfgw as ip_rfgw_rx, " +
                 "rx.port_rfgw as port_rfgw_rx, rx.mode as mode_rx, " +
                 "8c.date as date_8c, 8c.method as method_8c " +
                 "FROM srmRessource2v2cModel 2v " +
                 "LEFT JOIN usrm_vermserver_rx rx " +
                 "ON rx.ondemand_session_id = 2v.ondemand_session_id and rx.bitrate is not null " +
                 "LEFT JOIN usrm_vermserver_tx tx " +
                 "ON rx.ondemand_session_id = tx.ondemand_session_id and tx.code != \"teardown_or_ok\"  and tx.code != \"error\" " +
                 "LEFT JOIN srmEnd8cModel 8c " +
                 "ON rx.ondemand_session_id = 8c.ondemand_session_id ").repartition(1);

         sqlDF.show();
         sqlDF.write().csv("C:\\temp\\result2.csv");
    }

    private Dataset<Row> getDframe(String rootcsv, String logcomponent, String model, StructType schema, String day) {
        String filename = rootcsv + "\\output_logstash\\"+ logcomponent +"\\"+ model + "\\" + day + "\\data.csv";
        return spark.read()
                .format("csv")
                .option("header", "false")
                .option("delimiter", ";")
                .option("nullValue", "\"\"")
                .schema(schema)
                .load(filename);
    }
}
