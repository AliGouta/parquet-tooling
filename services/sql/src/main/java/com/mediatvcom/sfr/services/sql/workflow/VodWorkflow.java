package com.mediatvcom.sfr.services.sql.workflow;

import com.mediatvcom.sfr.services.sql.utils.models.srm.*;
import com.mediatvcom.sfr.services.sql.utils.models.streamer.StreamerModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverRxModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverTxModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.callUDF;

/**
 * Created by AGOUTA on 22/08/2016.
 */
public class VodWorkflow implements Serializable {

    private final SparkSession spark;
    private String rootCsv;
    private String day;

    private final SrmSessionStart4cModel srmSessionStart4cModel;
    Dataset<Row> df_srmSessionStart4cModel;
    /*
    Models used by the VoD Workflow
     */
    private final SrmSetup1vModel srmSetup1vModel;
    private final SrmRessource2v2cModel srmRessource2v2cModel;
    private final UsrmVermserverRxModel usrmVermserverRxModel;
    private final UsrmVermserverTxModel usrmVermserverTxModel;
    private final StreamerModel streamerModel;
    private final SrmSessionId3v5cModel srmSessionId3v5cModel;
    private final SrmTunningStartSession4v6v4s6cModel srmTunningStartSession4v6v4s6cModel;
    private final SrmSessionStart5vModel srmSessionStart5vModel;
    private final SrmEnd7v7cModel srmEnd7v7cModel;

    //,rtsp://10.212.16.138:554/?AssetId=M6R0006755260.mpg

    Pattern patternSessionId = Pattern.compile("Session: (.*);timeout=300");
    Pattern patternUrlToContent = Pattern.compile("(.+?)AssetId=(.*)");
    Pattern patternUrlToContentOnSetup = Pattern.compile("rtsp://localhost:8184/(.+?)(\\?definition.*|$)");

    Pattern patternClientId = Pattern.compile("(.+?)\\.(.*)");
    Pattern patternClientIdOnSetup = Pattern.compile("(\\d+?)\\.(\\d+?)\\.(.*)");

    /*
    DataFrames for VoD
     */
    Dataset<Row> df_srmSetup1vModel;
    Dataset<Row> df_srmRessource2v2cModel;
    Dataset<Row> df_usrm_vermserver_rx;
    Dataset<Row> df_usrm_vermserver_tx;
    Dataset<Row> df_streamerModel;
    Dataset<Row> df_srmSessionId3v5cModel;
    Dataset<Row> df_srmTunningStartSession4v6v4s6cModel;
    Dataset<Row> df_srmSessionStart5vModel;
    Dataset<Row> df_srmEnd7v7cModel;


    public VodWorkflow(SparkSession spark, String rootCsv, String day) {
        this.spark = spark;
        this.rootCsv = rootCsv;
        this.day = day;

        this.srmSessionStart4cModel = new SrmSessionStart4cModel(day, rootCsv);
        this.df_srmSessionStart4cModel = getDframe(srmSessionStart4cModel.getRootCsv(), srmSessionStart4cModel.getLogComponent(), srmSessionStart4cModel.getModelName(), srmSessionStart4cModel.getSchema(), day);

        this.srmSetup1vModel = new SrmSetup1vModel(day, rootCsv);
        this.srmRessource2v2cModel = new SrmRessource2v2cModel(day,rootCsv);
        this.usrmVermserverRxModel = new UsrmVermserverRxModel(day, rootCsv);
        this.usrmVermserverTxModel  = new UsrmVermserverTxModel(day, rootCsv);
        this.streamerModel = new StreamerModel(day, rootCsv);
        this.srmSessionId3v5cModel = new SrmSessionId3v5cModel(day,rootCsv);
        this.srmTunningStartSession4v6v4s6cModel = new SrmTunningStartSession4v6v4s6cModel(day, rootCsv);
        this.srmSessionStart5vModel = new SrmSessionStart5vModel(day, rootCsv);
        this.srmEnd7v7cModel  = new SrmEnd7v7cModel(day, rootCsv);

        this.df_srmSetup1vModel = getDframe(srmSetup1vModel.getRootCsv(), srmSetup1vModel.getLogComponent(), srmSetup1vModel.getModelName(),srmSetup1vModel.getSchema(), day);
        this.df_srmRessource2v2cModel = getDframe(srmRessource2v2cModel.getRootCsv(), srmRessource2v2cModel.getLogComponent(), srmRessource2v2cModel.getModelName(),srmRessource2v2cModel.getSchema(), day);
        this.df_usrm_vermserver_rx = getDframe(usrmVermserverRxModel.getRootCsv(), usrmVermserverRxModel.getLogComponent(), usrmVermserverRxModel.getModelName(), usrmVermserverRxModel.getSchema(), day);
        this.df_usrm_vermserver_tx = getDframe(usrmVermserverTxModel.getRootCsv(), usrmVermserverTxModel.getLogComponent(), usrmVermserverTxModel.getModelName(), usrmVermserverTxModel.getSchema(), day);
        this.df_streamerModel    = getDframe(streamerModel.getRootCsv(), streamerModel.getLogComponent(), streamerModel.getModelName(), streamerModel.getSchema(), day);
        this.df_srmSessionId3v5cModel = getDframe(srmSessionId3v5cModel.getRootCsv(), srmSessionId3v5cModel.getLogComponent(), srmSessionId3v5cModel.getModelName(), srmSessionId3v5cModel.getSchema(), day);
        this.df_srmTunningStartSession4v6v4s6cModel = getDframe(srmTunningStartSession4v6v4s6cModel.getRootCsv(), srmTunningStartSession4v6v4s6cModel.getLogComponent(), srmTunningStartSession4v6v4s6cModel.getModelName(), srmTunningStartSession4v6v4s6cModel.getSchema(), day);
        this.df_srmSessionStart5vModel = getDframe(srmSessionStart5vModel.getRootCsv(), srmSessionStart5vModel.getLogComponent(), srmSessionStart5vModel.getModelName(), srmSessionStart5vModel.getSchema(), day);
        this.df_srmEnd7v7cModel    = getDframe(srmEnd7v7cModel.getRootCsv(), srmEnd7v7cModel.getLogComponent(), srmEnd7v7cModel.getModelName(),srmEnd7v7cModel.getSchema(), day);

    }

     public void  runWorkflow(){

         df_srmRessource2v2cModel.createOrReplaceTempView("srmRessource2v2cModel");
         df_usrm_vermserver_rx.createOrReplaceTempView("usrm_vermserver_rx");
         df_usrm_vermserver_tx.createOrReplaceTempView("usrm_vermserver_tx");
         df_srmSessionId3v5cModel.createOrReplaceTempView("srmSessionId3v5cModel");
         df_srmTunningStartSession4v6v4s6cModel.createOrReplaceTempView("srmTunningStartSession4v6v4s6cModel");
         df_srmSessionStart5vModel.createOrReplaceTempView("srmSessionStart5vModel");
         df_srmEnd7v7cModel.createOrReplaceTempView("srmEnd7v7cModel");

         df_srmSessionStart4cModel.createOrReplaceTempView("srmSessionStart4cModel");

         spark.udf().register("getContentName", new UDF1<String, String>() {
             private static final long serialVersionUID = -5372447039252716846L;

             @Override
             public String call(String s) {
                 Matcher m = patternUrlToContent.matcher(s);
                 if (m.find()){
                     return m.group(2);
                 }
                 return null;
             }
         }, DataTypes.StringType);

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

         spark.udf().register("getCarteOnSetup", new UDF1<String, String>() {
             private static final long serialVersionUID = -5372447039252716846L;

             @Override
             public String call(String s) {
                 Matcher m = patternClientIdOnSetup.matcher(s);
                 if (m.find()){
                     return m.group(3);
                 }
                 return null;
             }
         }, DataTypes.StringType);

         spark.udf().register("getContentNameOnSetup", new UDF1<String, String>() {
             private static final long serialVersionUID = -5372447039252716846L;

             @Override
             public String call(String s) {
                 Matcher m = patternUrlToContentOnSetup.matcher(s);
                 if (m.find()){
                     return m.group(1);
                 }
                 return null;
             }
         }, DataTypes.StringType);

         df_streamerModel = df_streamerModel.withColumn("content_name", callUDF("getContentName", df_streamerModel.col("url")));
         df_streamerModel = df_streamerModel.withColumn("carteId", callUDF("getCarte", df_streamerModel.col("client_id")));
         df_streamerModel.createOrReplaceTempView("streamerModel");

         df_srmSetup1vModel = df_srmSetup1vModel.withColumn("carteId", callUDF("getCarteOnSetup", df_srmSetup1vModel.col("client_id_reverse")));
         df_srmSetup1vModel = df_srmSetup1vModel.withColumn("content_name", callUDF("getContentNameOnSetup", df_srmSetup1vModel.col("url")));
         df_srmSetup1vModel.createOrReplaceTempView("srmSetup1vModel");



         Dataset<Row> sqlDF4v = spark.sql("SELECT MIN(date) as date_min_4v, MAX(date) as date_max_4v, " +
                 "4v.session_id as sessionId_4v, " +
                 "tsid as tsid_4v, svcid as svcid_4v, " +
                 "content_type as content_type_4v " +
                 "From srmTunningStartSession4v6v4s6cModel 4v " +
                 "Group by session_id, tsid, svcid, content_type").cache();

         sqlDF4v.createOrReplaceTempView("4v");


         Dataset<Row> sqlcommonDF = spark.sql("SELECT rx.date as date_rx, 2v.date as date_2v, 2v.ondemand_session_id as ondemand_session_id_2v, " +
                 "2v.code_http as code_http_2v, rx.bitrate as bitrate_rx, rx.service_group as service_group_rx, tx.ip_rfgw as ip_rfgw_tx, " +
                 "tx.port_rfgw as port_rfgw_tx, rx.mode as mode_rx " +
                 "FROM srmRessource2v2cModel 2v " +
                 "LEFT JOIN usrm_vermserver_rx rx " +
                 "ON rx.ondemand_session_id = 2v.ondemand_session_id and rx.bitrate is not null " +
                 "LEFT JOIN usrm_vermserver_tx tx " +
                 "ON rx.ondemand_session_id = tx.ondemand_session_id and tx.code != \"teardown_or_ok\"  and tx.code != \"error\" ").cache();

         sqlcommonDF.createOrReplaceTempView("common");

         Dataset<Row> sqlDF = spark.sql("SELECT 1v.date as date_1v, " +
                 "3av.port_rfgw as port_rfgw_3av, 3av.ip_rfgw as ip_rfgw_3av, " +
                 "3av.date as date_3av, 3av.content_name as content_name_3av, 3av.carteId as carteId_3av, " +
                 "3v.date as date_3v, 3v.session_id as sessionId_3v, 3v.client_id as client_id_3v, " +
                 "tsid_4v, svcid_4v, content_type_4v, " +
                 "5v.vip as vip_5v, 5v.content_name as content_name_5v, 5v.content_type as content_type_5v, " +
                 "7v.date as date_7v, 7v.session_id as sessionId_7v, " +
                 "common.port_rfgw_tx as port_rfgw_tx_common, common.ip_rfgw_tx as ip_rfgw_tx_common, common.bitrate_rx as bitrate_rx_common, common.date_rx as date_rx_common " +
                 "From streamerModel 3av " +
                 "LEFT JOIN srmSessionId3v5cModel 3v " +
                 "ON 3av.cseq = 3v.cseq " +
                 "LEFT JOIN 4v " +
                 "ON 3v.session_id = 4v.sessionId_4v " +
                 "LEFT JOIN srmSessionStart5vModel 5v " +
                 "ON 3v.session_id = 5v.session_id " +
                 "LEFT JOIN srmEnd7v7cModel 7v " +
                 "ON 3v.session_id = 7v.session_id " +
                 "JOIN srmSetup1vModel 1v " +
                 "ON 1v.content_name = 3av.content_name and 1v.carteId = 3av.carteId  " +
                 "and unix_timestamp(3av.date, \"yyyy-MM-dd HH:mm:ss.SSS\") >= unix_timestamp(1v.date, \"yyyy-MM-dd HH:mm:ss,SSS\") " +
                 "and unix_timestamp(3av.date, \"yyyy-MM-dd HH:mm:ss.SSS\") < ( unix_timestamp(1v.date, \"yyyy-MM-dd HH:mm:ss,SSS\") + 60 ) " +
                 "LEFT JOIN common " +
                 "ON common.port_rfgw_tx = 3av.port_rfgw and common.ip_rfgw_tx = 3av.ip_rfgw " +
                 "and unix_timestamp(3av.date, \"yyyy-MM-dd HH:mm:ss.SSS\") >= unix_timestamp(common.date_2v, \"yyyy-MM-dd HH:mm:ss.SSS\") " +
                 "and unix_timestamp(3av.date, \"yyyy-MM-dd HH:mm:ss.SSS\") < ( unix_timestamp(common.date_2v, \"yyyy-MM-dd HH:mm:ss.SSS\") + 10 ) ").repartition(1);

         /*
         Dataset<Row> sqlDF = spark.sql("SELECT 3av.date as date_3av, 3av.content_name as content_name_3av, " +
                 "3v.date as date_3v, 3v.session_id as sessionId_3v, 3v.client_id as client_id_3v, " +
                 "tsid_4v, svcid_4v, content_type_4v, " +
                 "5v.vip as vip_5v, 5v.content_name as content_name_5v, 5v.content_type as content_type_5v, " +
                 "7v.date as date_7v, 7v.session_id as sessionId_7v " +
                 "From streamerModel 3av " +
                 "LEFT JOIN srmSessionId3v5cModel 3v " +
                 "ON 3av.cseq = 3v.cseq " +
                 "LEFT JOIN 4v " +
                 "ON 3v.session_id = 4v.sessionId_4v " +
                 "LEFT JOIN srmSessionStart5vModel 5v " +
                 "ON 3v.session_id = 5v.session_id " +
                 "LEFT JOIN srmEnd7v7cModel 7v " +
                 "ON 3v.session_id = 7v.session_id " +
                 "JOIN srmSetup1vModel 1v " +
                 "ON 1v.content_name = 3av.content_name and 1v.carteId = 3av.content_name " +
                 "and unix_timestamp(1v.date, \"yyyy-MM-dd HH:mm:ss,SSS\") >= unix_timestamp(3av.date, \"yyyy-MM-dd HH:mm:ss.SSS\") " +
                 "and unix_timestamp(1v.date, \"yyyy-MM-dd HH:mm:ss,SSS\") < ( unix_timestamp(3av.date, \"yyyy-MM-dd HH:mm:ss.SSS\") + 60 ) ").repartition(1);
*/

         sqlDF.show();
         sqlDF.write().csv("C:\\temp\\result1.csv");

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
