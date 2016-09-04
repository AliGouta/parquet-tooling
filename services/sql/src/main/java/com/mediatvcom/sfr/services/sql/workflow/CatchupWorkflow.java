package com.mediatvcom.sfr.services.sql.workflow;

import com.mediatvcom.sfr.services.sql.utils.models.srm.*;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverRxModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverTxModel;
import com.mediatvcom.sfr.services.sql.utils.udfs.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.*;

/**
 * Created by AGOUTA on 22/08/2016.
 */
public class CatchupWorkflow implements Serializable {

    private final SparkSession spark;
    List<String> dateRange;

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
    private final SrmTunningStartSession4v6v6s6cModel srmTunningStartSession4V6v6S6CModel;
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


    public CatchupWorkflow(SparkSession spark, String rootCsv, List<String> daterange) {
        this.spark = spark;
        this.dateRange = daterange;

        this.srmGetContent0cModel = new SrmGetContent0cModel(rootCsv);
        this.srmPostContent1cModel = new SrmPostContent1cModel(rootCsv);
        this.srmRessource2v2cModel = new SrmRessource2v2cModel(rootCsv);
        this.usrmVermserverRxModel = new UsrmVermserverRxModel(rootCsv);
        this.usrmVermserverTxModel  = new UsrmVermserverTxModel(rootCsv);
        this.srmSessionStart4cModel = new SrmSessionStart4cModel(rootCsv);
        this.srmSessionId3v5cModel = new SrmSessionId3v5cModel(rootCsv);
        this.srmTunningStartSession4V6v6S6CModel = new SrmTunningStartSession4v6v6s6cModel(rootCsv);
        this.srmEnd7v7cModel  = new SrmEnd7v7cModel(rootCsv);
        this.srmEnd8cModel = new SrmEnd8cModel(rootCsv);

        this.df_srmGetContent0cModel = getDframe(srmGetContent0cModel.getRootCsv(), srmGetContent0cModel.getLogComponent(), srmGetContent0cModel.getModelName(), srmGetContent0cModel.getSchema(), dateRange);
        this.df_srmPostContent1cModel = getDframe(srmPostContent1cModel.getRootCsv(), srmPostContent1cModel.getLogComponent(), srmPostContent1cModel.getModelName(), srmPostContent1cModel.getSchema(), dateRange);
        this.df_srmRessource2v2cModel = getDframe(srmRessource2v2cModel.getRootCsv(), srmRessource2v2cModel.getLogComponent(), srmRessource2v2cModel.getModelName(),srmRessource2v2cModel.getSchema(), dateRange);
        this.df_usrm_vermserver_rx = getDframe(usrmVermserverRxModel.getRootCsv(), usrmVermserverRxModel.getLogComponent(), usrmVermserverRxModel.getModelName(), usrmVermserverRxModel.getSchema(), dateRange);
        this.df_usrm_vermserver_tx = getDframe(usrmVermserverTxModel.getRootCsv(), usrmVermserverTxModel.getLogComponent(), usrmVermserverTxModel.getModelName(), usrmVermserverTxModel.getSchema(), dateRange);
        this.df_srmSessionStart4cModel = getDframe(srmSessionStart4cModel.getRootCsv(), srmSessionStart4cModel.getLogComponent(), srmSessionStart4cModel.getModelName(), srmSessionStart4cModel.getSchema(), dateRange);
        this.df_srmSessionId3v5cModel = getDframe(srmSessionId3v5cModel.getRootCsv(), srmSessionId3v5cModel.getLogComponent(), srmSessionId3v5cModel.getModelName(), srmSessionId3v5cModel.getSchema(), dateRange);
        this.df_srmTunningStartSession4v6v4s6cModel = getDframe(srmTunningStartSession4V6v6S6CModel.getRootCsv(), srmTunningStartSession4V6v6S6CModel.getLogComponent(), srmTunningStartSession4V6v6S6CModel.getModelName(), srmTunningStartSession4V6v6S6CModel.getSchema(), dateRange);
        this.df_srmEnd7v7cModel = getDframe(srmEnd7v7cModel.getRootCsv(), srmEnd7v7cModel.getLogComponent(), srmEnd7v7cModel.getModelName(), srmEnd7v7cModel.getSchema(), dateRange);
        this.df_srmEnd8cModel = getDframe(srmEnd8cModel.getRootCsv(), srmEnd8cModel.getLogComponent(), srmEnd8cModel.getModelName(), srmEnd8cModel.getSchema(), dateRange);

    }

     public void  runWorkflow(){

         df_srmRessource2v2cModel.createOrReplaceTempView("srmRessource2v2cModel");
         df_usrm_vermserver_rx.createOrReplaceTempView("usrm_vermserver_rx");
         df_usrm_vermserver_tx.createOrReplaceTempView("usrm_vermserver_tx");
         /*
         df_srmSessionStart4cModel.createOrReplaceTempView("srmSessionStart4cModel");
         df_srmSessionId3v5cModel.createOrReplaceTempView("srmSessionId3v5cModel");
         df_srmTunningStartSession4v6v4s6cModel.createOrReplaceTempView("srmTunningStartSession4V6v6S6CModel");
         df_srmEnd7v7cModel.createOrReplaceTempView("srmEnd7v7cModel");
         */

         df_srmEnd8cModel = df_srmEnd8cModel.withColumn("content_type", lit("catchup"));
         df_srmEnd8cModel.createOrReplaceTempView("srmEnd8cModel");

         spark.udf().register("getCarte", new Carte(), DataTypes.StringType);

         spark.udf().register("getCarteOnGet", new CarteCatchupOnGET(), DataTypes.StringType);
         spark.udf().register("getContentNameOnGet", new ContentNameCatchupOnGET(), DataTypes.StringType);
         spark.udf().register("getNitCatchupOnGet", new NITCatchupOnGET(), DataTypes.StringType);

         df_srmGetContent0cModel = df_srmGetContent0cModel.withColumn("content_name", callUDF("getContentNameOnGet", df_srmGetContent0cModel.col("url")));
         df_srmGetContent0cModel = df_srmGetContent0cModel.withColumn("carteId", callUDF("getCarteOnGet", df_srmGetContent0cModel.col("url")));
         df_srmGetContent0cModel = df_srmGetContent0cModel.withColumn("nit", callUDF("getNitCatchupOnGet", df_srmGetContent0cModel.col("url")));
         df_srmGetContent0cModel = df_srmGetContent0cModel.withColumn("content_type", lit("catchup"));

         df_srmGetContent0cModel.createOrReplaceTempView("srmGetContent0cModel");

         df_srmPostContent1cModel = df_srmPostContent1cModel.withColumn("content_type", lit("catchup"));
         df_srmPostContent1cModel.createOrReplaceTempView("srmPostContent1cModel");

         df_srmSessionStart4cModel = df_srmSessionStart4cModel.withColumn("carteId", callUDF("getCarte", df_srmSessionStart4cModel.col("client_id")));

         df_srmSessionStart4cModel.createOrReplaceTempView("srmSessionStart4cModel");

         Dataset<Row> sql0cerrorDF = spark.sql("SELECT 0c.date as date,  NULL as date_start_video, 0c.content_type as content_type, " +
                 "0c.content_name as content_name, NULL as streamer, NULL as bitrate_rx, NULL as service_group, " +
                 "NULL as port_rfgw, NULL as ip_rfgw, NULL as client_id, 0c.carteId as carteId, NULL as date_end, " +
                 "NULL as sessionId, NULL as mode_rx, 0c.code_http as code_http, NULL as x_srm_error_message, NULL as ondemand_session_id " +
                 "FROM srmGetContent0cModel 0c " +
                 "WHERE 0c.code_http != \"200 OK\" ").cache();

         sql0cerrorDF.createOrReplaceTempView("cathup_0c_error");

         Dataset<Row> sql1cerrorDF = spark.sql("SELECT 1c.date as date,  NULL as date_start_video, 1c.content_type as content_type, " +
                 "NULL as content_name, NULL as streamer, NULL as bitrate_rx, NULL as service_group, " +
                 "NULL as port_rfgw, NULL as ip_rfgw, NULL as client_id, NULL as carteId, NULL as date_end, " +
                 "NULL as sessionId, NULL as mode_rx, 1c.code_http as code_http, NULL as x_srm_error_message, NULL as ondemand_session_id " +
                 "FROM srmPostContent1cModel 1c " +
                 "WHERE 1c.code_http != \"200 OK\" ").cache();

         sql1cerrorDF.createOrReplaceTempView("cathup_1c_error");

         Dataset<Row> sqlcatchupDF = spark.sql("SELECT rx.date as date_rx, 2v.date as date_2v, 2v.ondemand_session_id as ondemand_session_id_2v, " +
                 "2v.code_http as code_http_2v, rx.bitrate as bitrate_rx, rx.service_group as service_group_rx, rx.ip_rfgw as ip_rfgw_rx, " +
                 "rx.port_rfgw as port_rfgw_rx, rx.mode as mode_rx, " +
                 "tjoin.date_8c as date_8c_tj, tjoin.content_type as content_type_8c " +
                 "FROM srmRessource2v2cModel 2v " +
                 "LEFT JOIN usrm_vermserver_rx rx " +
                 "ON rx.ondemand_session_id = 2v.ondemand_session_id and rx.bitrate is not null " +
                 "LEFT JOIN usrm_vermserver_tx tx " +
                 "ON rx.ondemand_session_id = tx.ondemand_session_id and tx.code != \"teardown_or_ok\"  and tx.code != \"error\" " +
                 "JOIN ( SELECT Min(8c.date) as date_8c, 8c.ondemand_session_id as ondemand_session_id_8c, content_type " +
                 "from srmEnd8cModel 8c " +
                 "Group by ondemand_session_id, content_type) tjoin " +
                 "ON rx.ondemand_session_id = tjoin.ondemand_session_id_8c ").cache();

         sqlcatchupDF.createOrReplaceTempView("cathup_success_session");

         Dataset<Row> sqlcatchupallDF = spark.sql("SELECT catchupok.date_rx as date, 4c.date as date_start_video, content_type_8c as content_type, content_name as content_name, NULL as streamer, " +
                 "catchupok.bitrate_rx as bitrate_rx, catchupok.service_group_rx as service_group, " +
                 "catchupok.port_rfgw_rx as port_rfgw, catchupok.ip_rfgw_rx as ip_rfgw, " +
                 "client_id as client_id, carteId as carteId, date_8c_tj as date_end, NULL as sessionId, catchupok.mode_rx as mode_rx, " +
                 "\"200 OK\" as code_http, NULL as x_srm_error_message, catchupok.ondemand_session_id_2v as ondemand_session_id " +
                 "FROM cathup_success_session catchupok " +
                 "LEFT JOIN srmSessionStart4cModel 4c " +
                 "ON 4c.control_session = catchupok.ondemand_session_id_2v " +
                 "UNION ALL " +
                 "SELECT * " +
                 "FROM cathup_0c_error " +
                 "UNION ALL " +
                 "SELECT * " +
                 "FROM cathup_1c_error ").repartition(1);


         sqlcatchupallDF.show();
         sqlcatchupallDF.write().csv("C:\\temp\\resultc.csv");
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
            files[i] = rootcsv + "\\output_logstash\\"+ logcomponent +"\\"+ model + "\\" + day + "\\data.csv";
            i++;
        }
        return files;
    }
}
