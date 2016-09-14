package com.mediatvcom.sfr.services.sql.workflow;

import com.mediatvcom.sfr.services.sql.utils.models.srm.*;
import com.mediatvcom.sfr.services.sql.utils.models.streamer.StreamerModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmSnmpModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmTransformedSnmpModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverRxModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverTxModel;
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
import static org.apache.spark.sql.functions.lit;

/**
 * Created by AGOUTA on 22/08/2016.
 */
public class NumericableVoDCatchup implements Serializable {

    private final SparkSession spark;
    private String rootCsv;
    List<String> dateRange;

    private final String esNode;
    private final String esPort;
    private final String indexVodCatchupAll;
    private final String indexCanal;

    private final SrmGetContent0cModel srmGetContent0cModel;
    private final SrmPostContent1cModel srmPostContent1cModel;
    private final SrmSessionStart4cModel srmSessionStart4cModel;
    private final SrmEnd7v7cModel srmEnd7v7cModel;
    private final SrmEnd8cModel srmEnd8cModel;

    Dataset<Row> df_srmGetContent0cModel;
    Dataset<Row> df_srmPostContent1cModel;
    Dataset<Row> df_srmSessionStart4cModel;
    Dataset<Row> df_srmEnd7v7cModel;
    Dataset<Row> df_srmEnd8cModel;

    /*
    Models used by the VoD Workflow
     */
    private final SrmSetup1vModel srmSetup1vModel;
    private final SrmRessource2v2cModel srmRessource2v2cModel;
    private final UsrmSnmpModel usrmSnmpModel;
    private final UsrmTransformedSnmpModel usrmTransformedSnmpModel;
    private final UsrmVermserverRxModel usrmVermserverRxModel;
    private final UsrmVermserverTxModel usrmVermserverTxModel;
    private final StreamerModel streamerModel;
    private final SrmSessionId3v5cModel srmSessionId3v5cModel;
    private final SrmTunningStartSession4v6v6s6cModel srmTunningStartSession4V6v6S6CModel;
    private final SrmSessionStart5vModel srmSessionStart5vModel;

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
    Dataset<Row> df_usrmSnmpModel;


    public NumericableVoDCatchup(SparkSession spark, String rootCsv, List<String> daterange, String esnode, String esport, String indexcatchupvodall, String indexcanal) {
        this.spark = spark;
        this.dateRange = daterange;

        this.esNode = esnode;
        this.esPort = esport;
        this.indexVodCatchupAll = indexcatchupvodall;
        this.indexCanal = indexcanal;

        this.srmGetContent0cModel = new SrmGetContent0cModel(rootCsv);
        this.srmPostContent1cModel = new SrmPostContent1cModel(rootCsv);
        this.srmSessionStart4cModel = new SrmSessionStart4cModel(rootCsv);
        this.srmEnd7v7cModel  = new SrmEnd7v7cModel(rootCsv);
        this.srmEnd8cModel = new SrmEnd8cModel(rootCsv);

        this.df_srmGetContent0cModel = getDframe(srmGetContent0cModel.getRootCsv(), srmGetContent0cModel.getLogComponent(), srmGetContent0cModel.getModelName(), srmGetContent0cModel.getSchema(), dateRange);
        this.df_srmPostContent1cModel = getDframe(srmPostContent1cModel.getRootCsv(), srmPostContent1cModel.getLogComponent(), srmPostContent1cModel.getModelName(), srmPostContent1cModel.getSchema(), dateRange);
        this.df_srmSessionStart4cModel = getDframe(srmSessionStart4cModel.getRootCsv(), srmSessionStart4cModel.getLogComponent(), srmSessionStart4cModel.getModelName(), srmSessionStart4cModel.getSchema(), dateRange);
        this.df_srmEnd7v7cModel = getDframe(srmEnd7v7cModel.getRootCsv(), srmEnd7v7cModel.getLogComponent(), srmEnd7v7cModel.getModelName(), srmEnd7v7cModel.getSchema(), dateRange);
        this.df_srmEnd8cModel = getDframe(srmEnd8cModel.getRootCsv(), srmEnd8cModel.getLogComponent(), srmEnd8cModel.getModelName(), srmEnd8cModel.getSchema(), dateRange);

        this.srmSetup1vModel = new SrmSetup1vModel(rootCsv);
        this.srmRessource2v2cModel = new SrmRessource2v2cModel(rootCsv);
        this.usrmSnmpModel    = new UsrmSnmpModel(rootCsv);
        this.usrmTransformedSnmpModel = new UsrmTransformedSnmpModel(rootCsv);
        this.usrmVermserverRxModel = new UsrmVermserverRxModel(rootCsv);
        this.usrmVermserverTxModel  = new UsrmVermserverTxModel(rootCsv);
        this.streamerModel = new StreamerModel(rootCsv);
        this.srmSessionId3v5cModel = new SrmSessionId3v5cModel(rootCsv);
        this.srmTunningStartSession4V6v6S6CModel = new SrmTunningStartSession4v6v6s6cModel(rootCsv);
        this.srmSessionStart5vModel = new SrmSessionStart5vModel(rootCsv);

        this.df_srmSetup1vModel = getDframe(srmSetup1vModel.getRootCsv(), srmSetup1vModel.getLogComponent(), srmSetup1vModel.getModelName(),srmSetup1vModel.getSchema(), dateRange);
        this.df_srmRessource2v2cModel = getDframe(srmRessource2v2cModel.getRootCsv(), srmRessource2v2cModel.getLogComponent(), srmRessource2v2cModel.getModelName(),srmRessource2v2cModel.getSchema(), dateRange);
        this.df_usrmSnmpModel = getDframe(usrmSnmpModel.getRootCsv(), usrmSnmpModel.getLogComponent(), usrmSnmpModel.getModelName(),usrmSnmpModel.getSchema(), dateRange);
        this.df_usrm_vermserver_rx = getDframe(usrmVermserverRxModel.getRootCsv(), usrmVermserverRxModel.getLogComponent(), usrmVermserverRxModel.getModelName(), usrmVermserverRxModel.getSchema(), dateRange);
        this.df_usrm_vermserver_tx = getDframe(usrmVermserverTxModel.getRootCsv(), usrmVermserverTxModel.getLogComponent(), usrmVermserverTxModel.getModelName(), usrmVermserverTxModel.getSchema(), dateRange);
        this.df_streamerModel    = getDframe(streamerModel.getRootCsv(), streamerModel.getLogComponent(), streamerModel.getModelName(), streamerModel.getSchema(), dateRange);
        this.df_srmSessionId3v5cModel = getDframe(srmSessionId3v5cModel.getRootCsv(), srmSessionId3v5cModel.getLogComponent(), srmSessionId3v5cModel.getModelName(), srmSessionId3v5cModel.getSchema(), dateRange);
        this.df_srmTunningStartSession4v6v4s6cModel = getDframe(srmTunningStartSession4V6v6S6CModel.getRootCsv(), srmTunningStartSession4V6v6S6CModel.getLogComponent(), srmTunningStartSession4V6v6S6CModel.getModelName(), srmTunningStartSession4V6v6S6CModel.getSchema(), dateRange);
        this.df_srmSessionStart5vModel = getDframe(srmSessionStart5vModel.getRootCsv(), srmSessionStart5vModel.getLogComponent(), srmSessionStart5vModel.getModelName(), srmSessionStart5vModel.getSchema(), dateRange);
        this.df_srmEnd7v7cModel    = getDframe(srmEnd7v7cModel.getRootCsv(), srmEnd7v7cModel.getLogComponent(), srmEnd7v7cModel.getModelName(),srmEnd7v7cModel.getSchema(), dateRange);

    }

    public void  runWorkflow(){

        df_srmRessource2v2cModel.createOrReplaceTempView("srmRessource2v2cModel");
        df_usrm_vermserver_rx.createOrReplaceTempView("usrm_vermserver_rx");
        df_usrm_vermserver_tx.createOrReplaceTempView("usrm_vermserver_tx");
        df_srmSessionId3v5cModel.createOrReplaceTempView("srmSessionId3v5cModel");
        df_srmSessionStart5vModel.createOrReplaceTempView("srmSessionStart5vModel");
        df_srmEnd7v7cModel.createOrReplaceTempView("srmEnd7v7cModel");

        df_srmSessionStart4cModel.createOrReplaceTempView("srmSessionStart4cModel");

        df_srmEnd8cModel = df_srmEnd8cModel.withColumn("content_type", lit("catchup"));
        df_srmEnd8cModel.createOrReplaceTempView("srmEnd8cModel");

        spark.udf().register("getCarteOnGet", new CarteCatchupOnGET(), DataTypes.StringType);
        spark.udf().register("getContentNameOnGet", new ContentNameCatchupOnGET(), DataTypes.StringType);
        spark.udf().register("getNitCatchupOnGet", new NITCatchupOnGET(), DataTypes.StringType);
        spark.udf().register("getContentName", new ContentNameUDF(), DataTypes.StringType);
        spark.udf().register("getCarte", new Carte(), DataTypes.StringType);
        spark.udf().register("getCarteOn4vError", new CarteOn4vError(), DataTypes.StringType);
        spark.udf().register("getContentNameOnSetupOn4vError", new ContentNameOnSetupOn4vError(), DataTypes.StringType);
        spark.udf().register("getContentNameOnSetup", new ContentNameOnSetup(), DataTypes.StringType);
        spark.udf().register("getCarteOnSetup", new CarteOnSetup(), DataTypes.StringType);
        spark.udf().register("getBitrate", new Bitrate(), DataTypes.LongType);

        df_srmGetContent0cModel = df_srmGetContent0cModel.withColumn("content_name", callUDF("getContentNameOnGet", df_srmGetContent0cModel.col("url")));
        df_srmGetContent0cModel = df_srmGetContent0cModel.withColumn("carteId", callUDF("getCarteOnGet", df_srmGetContent0cModel.col("url")));
        df_srmGetContent0cModel = df_srmGetContent0cModel.withColumn("nit", callUDF("getNitCatchupOnGet", df_srmGetContent0cModel.col("url")));
        df_srmGetContent0cModel = df_srmGetContent0cModel.withColumn("content_type", lit("catchup"));
        df_srmGetContent0cModel.createOrReplaceTempView("srmGetContent0cModel");

        df_srmPostContent1cModel = df_srmPostContent1cModel.withColumn("content_type", lit("catchup"));
        df_srmPostContent1cModel.createOrReplaceTempView("srmPostContent1cModel");

        df_srmSessionStart4cModel = df_srmSessionStart4cModel.withColumn("carteId", callUDF("getCarte", df_srmSessionStart4cModel.col("client_id")));
        df_srmSessionStart4cModel.createOrReplaceTempView("srmSessionStart4cModel");


        df_streamerModel = df_streamerModel.withColumn("content_name", callUDF("getContentName", df_streamerModel.col("url")));
        df_streamerModel = df_streamerModel.withColumn("carteId", callUDF("getCarte", df_streamerModel.col("client_id")));
        df_streamerModel.createOrReplaceTempView("streamerModel");

        df_srmSetup1vModel = df_srmSetup1vModel.withColumn("carteId", callUDF("getCarteOnSetup", df_srmSetup1vModel.col("client_id_reverse")));
        df_srmSetup1vModel = df_srmSetup1vModel.withColumn("content_name", callUDF("getContentNameOnSetup", df_srmSetup1vModel.col("url")));
        df_srmSetup1vModel = df_srmSetup1vModel.withColumn("content_type", lit("vod"));
        df_srmSetup1vModel.createOrReplaceTempView("srmSetup1vModel");

        df_srmTunningStartSession4v6v4s6cModel = df_srmTunningStartSession4v6v4s6cModel.withColumn("carteId", callUDF("getCarteOn4vError", df_srmTunningStartSession4v6v4s6cModel.col("x_srm_error_message"), df_srmTunningStartSession4v6v4s6cModel.col("url"), df_srmTunningStartSession4v6v4s6cModel.col("client_id")));
        df_srmTunningStartSession4v6v4s6cModel = df_srmTunningStartSession4v6v4s6cModel.withColumn("content_name", callUDF("getContentNameOnSetupOn4vError", df_srmTunningStartSession4v6v4s6cModel.col("url")));
        df_srmTunningStartSession4v6v4s6cModel.createOrReplaceTempView("srmTunningStartSession4V6v6S6CModel");

        SnmpTransfrom snmpTransfrom = new SnmpTransfrom(df_usrmSnmpModel);


         /*
         Catchup errors 0c
          */


        Dataset<Row> sql0cerrorDF = spark.sql("SELECT from_utc_timestamp(from_unixtime(unix_timestamp(0c.date, \"yyyy-MM-dd HH:mm:ss.SSS\")), 'Europe/Paris') as date,  " +
                "NULL as date_start_video, 0c.content_type as content_type, " +
                "0c.content_name as content_name, " +
                "if (0c.content_name rlike 'PUB.*', \"true\", \"false\" ) as is_pub, " +
                "NULL as streamer, NULL as bitrate_rx, NULL as rfgw_id, NULL as service_group, " +
                "NULL as port_rfgw, NULL as ip_rfgw, NULL as client_id, 0c.carteId as carteId, NULL as date_end, " +
                "NULL as sessionId, NULL as mode_rx, 0c.code_http as code_http, NULL as x_srm_error_message, NULL as ondemand_session_id " +
                "FROM srmGetContent0cModel 0c " +
                "WHERE 0c.code_http != \"200 OK\" and to_date(0c.date) = to_date( " + "\"" + dateRange.get(0) + "\"" + " ) ").cache();

        sql0cerrorDF.createOrReplaceTempView("cathup_0c_error");



        Dataset<Row> sql1cerrorDF = spark.sql("SELECT from_utc_timestamp(from_unixtime(unix_timestamp(1c.date, \"yyyy-MM-dd HH:mm:ss.SSS\")), 'Europe/Paris') as date,  " +
                "NULL as date_start_video, 1c.content_type as content_type, " +
                "NULL as content_name, \"false\" as is_pub, NULL as streamer, NULL as bitrate_rx, NULL as rfgw_id, NULL as service_group, " +
                "NULL as port_rfgw, NULL as ip_rfgw, NULL as client_id, NULL as carteId, NULL as date_end, " +
                "NULL as sessionId, NULL as mode_rx, 1c.code_http as code_http, NULL as x_srm_error_message, NULL as ondemand_session_id " +
                "FROM srmPostContent1cModel 1c " +
                "WHERE 1c.code_http != \"200 OK\" and to_date(1c.date) = to_date( " + "\"" + dateRange.get(0) + "\"" + " ) ").cache();

        sql1cerrorDF.createOrReplaceTempView("cathup_1c_error");


        Dataset<Row> sqlVodCatchuperrorDF = spark.sql("SELECT from_utc_timestamp(from_unixtime(unix_timestamp(date_4v, \"yyyy-MM-dd HH:mm:ss.SSS\")), 'Europe/Paris') as date, " +
                "NULL as date_start_video,  " +
                "if (content_name_4v rlike '(.+?)controlSession.*', \"catchup\", content_type_1v) as content_type, " +
                "content_name_4v as content_name, " +
                "\"false\" as is_pub, " +
                "NULL as streamer, NULL as bitrate_rx, NULL as rfgw_id, NULL as service_group, NULL as port_rfgw, NULL as ip_rfgw, NULL as client_id, " +
                "carteId_4v as carteId, NULL as date_end, NULL as sessionId, NULL as mode_rx, " +
                "code_http_4v as code_http, x_srm_error_message_4v as x_srm_error_message, NULL as ondemand_session_id " +
                "FROM ( " +
                    "SELECT 1v.date as date_1v, " +
                    "4v.date as date_4v, 4v.code_http as code_http_4v, 4v.x_srm_error_message as x_srm_error_message_4v, 4v.carteId as carteId_4v, " +
                    "4v.content_name as content_name_4v, 1v.content_type as content_type_1v " +
                    "FROM ( " +
                        "SELECT tr4v.date as date, tr4v.code_http as code_http, tr4v.x_srm_error_message as x_srm_error_message, " +
                        "tr4v.carteId as carteId, tr4v.content_name as content_name, tr4v.error as error " +
                        "FROM srmTunningStartSession4V6v6S6CModel tr4v " +
                        "WHERE to_date(tr4v.date) = to_date(  " + "\"" + dateRange.get(0) + "\"" + " ) " +
                    ") 4v " +
                    "LEFT JOIN srmSetup1vModel 1v " +
                    "ON 1v.content_name = 4v.content_name and 1v.carteId = 4v.carteId " +
                    "and unix_timestamp(4v.date, \"yyyy-MM-dd HH:mm:ss.SSS\") >= unix_timestamp(1v.date, \"yyyy-MM-dd HH:mm:ss.SSS\" ) " +
                    "where 4v.error = \"true\" and 4v.content_name rlike '^[A-Z].*' " +
                ") errors " +
                "GROUP BY date_4v, code_http_4v, x_srm_error_message_4v, carteId_4v, content_name_4v, content_type_1v ").cache();

        sqlVodCatchuperrorDF.createOrReplaceTempView("vod_catchup_error");


         /*
         VoD & catchup
          */

        // , cseq should be removed since it is somtimes set to some value!
        Dataset<Row> sqlDF4v = spark.sql("SELECT MIN(date) as date_min_4v, MAX(date) as date_max_4v, " +
                "4v.session_id as sessionId_4v, " +
                "tsid as tsid_4v, svcid as svcid_4v, " +
                "content_type as content_type_4v " +
                "From srmTunningStartSession4V6v6S6CModel 4v " +
                "Group by session_id, tsid, svcid, content_type ").cache();

        sqlDF4v.createOrReplaceTempView("4v");

        Dataset<Row> sqlcommonDF = spark.sql("SELECT rx.date as date_rx, 2v.date as date_2v, 2v.ondemand_session_id as ondemand_session_id_2v, " +
                "2v.code_http as code_http_2v, rx.bitrate as bitrate_rx, regexp_extract(rx.service_group, \"(.+?_U)(.*)\", 1) as rfgw_id, rx.service_group as service_group_rx, tx.ip_rfgw as ip_rfgw_tx, " +
                "tx.port_rfgw as port_rfgw_tx, rx.mode as mode_rx " +
                "FROM ( " +
                    "SELECT tr2v.date as date, tr2v.ondemand_session_id as ondemand_session_id, " +
                    "tr2v.code_http as code_http " +
                    "FROM srmRessource2v2cModel tr2v " +
                    "WHERE to_date(tr2v.date) = to_date( " + "\"" + dateRange.get(0) + "\"" + " ) " +
                ") 2v " +
                "LEFT JOIN usrm_vermserver_rx rx " +
                "ON rx.ondemand_session_id = 2v.ondemand_session_id and rx.bitrate is not null " +
                "LEFT JOIN usrm_vermserver_tx tx " +
                "ON rx.ondemand_session_id = tx.ondemand_session_id and tx.code != \"teardown_or_ok\"  and tx.code != \"error\" ").cache();

        sqlcommonDF.createOrReplaceTempView("common");


        Dataset<Row> sqlDF3a1v = spark.sql("SELECT MAX(1vus.date) as date_1vus, 1vus.content_type as content_type_1vus, 3aus.date as date_3aus, " +
                "3aus.cseq as cseq_3aus, 3aus.client_id as client_id_3aus, 3aus.streamer as streamer_3aus, " +
                "3aus.content_name as content_name_3aus, 3aus.carteId as carteId_3aus " +
                "From ( " +
                    "SELECT tr3aus.date as date, tr3aus.cseq as cseq, tr3aus.client_id as client_id, tr3aus.streamer as streamer, " +
                    "tr3aus.content_name as content_name, tr3aus.carteId as carteId  " +
                    "FROM streamerModel tr3aus " +
                    "WHERE to_date(tr3aus.date) = to_date( " + "\"" + dateRange.get(0) + "\"" + " )" +
                ") 3aus " +
                "JOIN srmSetup1vModel 1vus " +
                "ON 1vus.content_name = 3aus.content_name and 1vus.carteId = 3aus.carteId " +
                "and ( unix_timestamp(3aus.date, \"yyyy-MM-dd HH:mm:ss\") + 1 ) >= unix_timestamp(1vus.date, \"yyyy-MM-dd HH:mm:ss.SSS\") " +
                "and ( unix_timestamp(3aus.date, \"yyyy-MM-dd HH:mm:ss\") + 1 ) < ( unix_timestamp(1vus.date, \"yyyy-MM-dd HH:mm:ss.SSS\") + 10 ) " +
                "GROUP BY 3aus.client_id, 3aus.streamer, 3aus.cseq, 3aus.content_name, 3aus.date, 3aus.carteId, 1vus.content_type ").repartition(1);

        sqlDF3a1v.createOrReplaceTempView("3aus1vus");

        Dataset<Row> sqlDF3aCommon = spark.sql("SELECT MAX(common.date_rx) as date_common, 3aus.date as date_3aus, 3aus.client_id as client_id_3aus, " +
                "3aus.ip_rfgw as ip_rfgw_3aus, 3aus.port_rfgw as port_rfgw_3aus, 3aus.content_name as content_name_3aus, " +
                "common.bitrate_rx as bitrate_rx_common, common.rfgw_id as rfgw_id, common.service_group_rx as service_group_rx_common, common.mode_rx as mode_rx_3aus " +
                "From ( " +
                    "SELECT tr3aus.date as date, tr3aus.client_id as client_id, " +
                    "tr3aus.ip_rfgw as ip_rfgw, tr3aus.port_rfgw as port_rfgw, tr3aus.content_name as content_name," +
                    "tr3aus.cseq as cseq " +
                    "FROM streamerModel tr3aus " +
                    "WHERE to_date(tr3aus.date) = to_date( " + "\"" + dateRange.get(0) + "\"" + " ) " +
                ") 3aus " +
                "JOIN common " +
                "ON common.port_rfgw_tx = 3aus.port_rfgw and common.ip_rfgw_tx = 3aus.ip_rfgw " +
                "and ( unix_timestamp(3aus.date, \"yyyy-MM-dd HH:mm:ss\") + 7201 ) >= unix_timestamp(common.date_rx, \"yyyy-MM-dd HH:mm:ss.SSS\")  " +
                "and ( unix_timestamp(3aus.date, \"yyyy-MM-dd HH:mm:ss\") + 7201 ) < ( unix_timestamp(common.date_rx, \"yyyy-MM-dd HH:mm:ss.SSS\") + 10 )  " +
                "Group by 3aus.client_id, 3aus.cseq, common.bitrate_rx, common.rfgw_id, common.service_group_rx, common.mode_rx, " +
                "3aus.content_name, 3aus.date, 3aus.ip_rfgw, 3aus.port_rfgw ").cache();

        sqlDF3aCommon.createOrReplaceTempView("3auscommon");

         /*
         Vod OK
          */

        Dataset<Row> sqlSucessVodlDF = spark.sql("SELECT from_utc_timestamp(from_unixtime(unix_timestamp(3aus1vus.date_1vus, \"yyyy-MM-dd HH:mm:ss.SSS\")), 'Europe/Paris') as date, " +
                "NULL as date_start_video, 3aus1vus.content_type_1vus as content_type, 3av.content_name as content_name, " +
                "if (3av.content_name rlike 'PUB.*', \"true\", \"false\" ) as is_pub, " +
                "3av.streamer as streamer, 3auscommon.bitrate_rx_common as bitrate_rx, 3auscommon.rfgw_id as rfgw_id, 3auscommon.service_group_rx_common as service_group, " +
                "3av.port_rfgw as port_rfgw, 3av.ip_rfgw as ip_rfgw, 3v.client_id as client_id, 3av.carteId as carteId, 7v.date as date_end, 3v.session_id as sessionId, " +
                "3auscommon.mode_rx_3aus as mode_rx, \"200 OK\" as code_http, NULL as x_srm_error_message, NULL as ondemand_session_id " +
                "From streamerModel 3av " +
                "LEFT JOIN srmSessionId3v5cModel 3v " +
                "ON 3av.cseq = 3v.cseq " +
                "AND ( unix_timestamp(3av.date, \"yyyy-MM-dd HH:mm:ss\") + 300 ) > unix_timestamp(3v.date, \"yyyy-MM-dd HH:mm:ss.SSS\") " +
                "AND unix_timestamp(3av.date, \"yyyy-MM-dd HH:mm:ss\") < ( unix_timestamp(3v.date, \"yyyy-MM-dd HH:mm:ss.SSS\") + 300 ) " +
                "LEFT JOIN 4v " +
                "ON 3v.session_id = 4v.sessionId_4v " +
                "LEFT JOIN srmSessionStart5vModel 5v " +
                "ON 3v.session_id = 5v.session_id " +
                "LEFT JOIN srmEnd7v7cModel 7v " +
                "ON 3v.session_id = 7v.session_id " +
                "JOIN 3aus1vus " +
                "ON 3aus1vus.content_name_3aus = 3av.content_name and 3aus1vus.client_id_3aus = 3av.client_id " +
                "and 3aus1vus.cseq_3aus = 3av.cseq and 3aus1vus.date_3aus = 3av.date " +
                "JOIN 3auscommon " +
                "ON 3auscommon.ip_rfgw_3aus = 3av.ip_rfgw and 3auscommon.port_rfgw_3aus = 3av.port_rfgw " +
                "and 3auscommon.date_3aus = 3av.date ").cache();

        sqlSucessVodlDF.createOrReplaceTempView("vod_sessions_ok");


        Dataset<Row> sqlcatchupDF = spark.sql("SELECT from_utc_timestamp(from_unixtime(unix_timestamp(rx.date, \"yyyy-MM-dd HH:mm:ss.SSS\")), 'UTC') as date_rx, " +
                "2v.date as date_2v, 2v.ondemand_session_id as ondemand_session_id_2v, " +
                "2v.code_http as code_http_2v, rx.bitrate as bitrate_rx, regexp_extract(rx.service_group, \"(.+?_U)(.*)\", 1) as rfgw_id, rx.service_group as service_group_rx, rx.ip_rfgw as ip_rfgw_rx, " +
                "rx.port_rfgw as port_rfgw_rx, rx.mode as mode_rx, " +
                "tjoin.date_8c as date_8c_tj, tjoin.content_type as content_type_8c " +
                "FROM ( " +
                    "SELECT tr2v.date as date, tr2v.ondemand_session_id as ondemand_session_id, " +
                    "tr2v.code_http as code_http " +
                    "FROM srmRessource2v2cModel tr2v " +
                    "WHERE to_date(tr2v.date) = to_date( " + "\"" + dateRange.get(0) + "\"" + " ) " +
                ") 2v " +
                "LEFT JOIN usrm_vermserver_rx rx " +
                "ON rx.ondemand_session_id = 2v.ondemand_session_id and rx.bitrate is not null " +
                "LEFT JOIN usrm_vermserver_tx tx " +
                "ON rx.ondemand_session_id = tx.ondemand_session_id and tx.code != \"teardown_or_ok\"  and tx.code != \"error\" " +
                "JOIN ( SELECT Min(8c.date) as date_8c, 8c.ondemand_session_id as ondemand_session_id_8c, content_type " +
                "from srmEnd8cModel 8c " +
                "Group by ondemand_session_id, content_type) tjoin " +
                "ON rx.ondemand_session_id = tjoin.ondemand_session_id_8c ").cache();

        sqlcatchupDF.createOrReplaceTempView("cathup_success_session");

        Dataset<Row> sqlallDF = spark.sql("SELECT catchupok.date_rx as date, 4c.date as date_start_video, content_type_8c as content_type, content_name as content_name, " +
                "if (content_name rlike 'PUB.*', \"true\", \"false\" ) as is_pub, " +
                "NULL as streamer, " +
                "catchupok.bitrate_rx as bitrate_rx, catchupok.rfgw_id as rfgw_id, catchupok.service_group_rx as service_group, " +
                "catchupok.port_rfgw_rx as port_rfgw, catchupok.ip_rfgw_rx as ip_rfgw, " +
                "client_id as client_id, carteId as carteId, date_8c_tj as date_end, NULL as sessionId, catchupok.mode_rx as mode_rx, " +
                "\"200 OK\" as code_http, NULL as x_srm_error_message, catchupok.ondemand_session_id_2v as ondemand_session_id " +
                "FROM cathup_success_session catchupok " +
                "LEFT JOIN srmSessionStart4cModel 4c " +
                "ON 4c.control_session = catchupok.ondemand_session_id_2v " +
                "UNION ALL " +
                "SELECT * " +
                "FROM vod_sessions_ok " +
                "UNION ALL " +
                "SELECT * " +
                "FROM cathup_0c_error " +
                "UNION ALL " +
                "SELECT * " +
                "FROM cathup_1c_error " +
                "UNION ALL " +
                "SELECT * " +
                "FROM vod_catchup_error ").cache();

        sqlallDF = sqlallDF.withColumn("bitrate_long_rx", callUDF("getBitrate", sqlallDF.col("bitrate_rx")));
        sqlallDF.createOrReplaceTempView("all_vod_catchup");


        Dataset<Row> sqlcanalDF = spark.sql("SELECT vodcatchup.date as date, vodcatchup.content_name as content_name, " +
                "vodcatchup.bitrate_long_rx as bitrate_long_rx, vodcatchup.bitrate_rx as bitrate_rx " +
                "FROM all_vod_catchup vodcatchup " +
                "WHERE vodcatchup.content_name like \"%COD%\" ").cache();

        sqlcanalDF.createOrReplaceTempView("canal_bw");

        Dataset<Row> sqlFinalCanalDF = spark.sql("SELECT " +
                "date, " +
                "if (content_name IS NOT NULL, content_name, \"null\") as content_name, " +
                "if (bitrate_rx IS NOT NULL, bitrate_long_rx, 0) as bitrate_rx " +
                "FROM canal_bw ").repartition(1);

        Dataset<Row> sqlFinalVodCatchupAllDF = spark.sql("SELECT " +
                "date, " +
                "if (date_start_video IS NOT NULL, date_start_video, \"null\") as date_start_video, " +
                "if (content_type IS NOT NULL, content_type, \"null\") as content_type, " +
                "if (content_name IS NOT NULL, content_name, \"null\") as content_name, " +
                "if (is_pub IS NOT NULL, is_pub, \"null\") as is_pub, " +
                "if (streamer IS NOT NULL, streamer, \"null\") as streamer, " +
                "if (bitrate_rx IS NOT NULL, bitrate_long_rx, 0) as bitrate_rx, " +
                "if (rfgw_id IS NOT NULL, rfgw_id, \"null\") as rfgw_id,  " +
                "if (service_group IS NOT NULL, service_group, \"null\") as service_group,  " +
                "if (port_rfgw IS NOT NULL, port_rfgw, \"null\") as port_rfgw, " +
                "if (ip_rfgw IS NOT NULL, ip_rfgw, \"null\") as ip_rfgw, " +
                "if (client_id IS NOT NULL, client_id, \"null\") as client_id, " +
                "if (carteId IS NOT NULL, carteId, \"null\") as carteId, " +
                "if (date_end IS NOT NULL, date_end, \"null\") as date_end, " +
                "if (sessionId IS NOT NULL, sessionId, \"null\") as sessionId, " +
                "if (mode_rx IS NOT NULL, mode_rx, \"null\") as mode_rx, " +
                "if (code_http IS NOT NULL, code_http, \"null\") as code_http, " +
                "if (x_srm_error_message IS NOT NULL, x_srm_error_message, \"null\") as x_srm_error_message, " +
                "if (ondemand_session_id IS NOT NULL, ondemand_session_id, \"null\") as ondemand_session_id " +
                "FROM all_vod_catchup ").repartition(1);


        /*
        root
         |-- date: timestamp (nullable = true)
         |-- date_start_video: string (nullable = true)
         |-- content_type: string (nullable = true)
         |-- content_name: string (nullable = true)
         |-- streamer: string (nullable = true)
         |-- bitrate_rx: long (nullable = true)
         |-- rfgw_id: string (nullable = true)
         |-- service_group: string (nullable = true)
         |-- port_rfgw: string (nullable = true)
         |-- ip_rfgw: string (nullable = true)
         |-- client_id: string (nullable = true)
         |-- carteId: string (nullable = true)
         |-- date_end: string (nullable = true)
         |-- sessionId: string (nullable = true)
         |-- mode_rx: string (nullable = true)
         |-- code_http: string (nullable = true)
         |-- x_srm_error_message: string (nullable = true)
         |-- ondemand_session_id: string (nullable = true)
         */

        /*
        root
         |-- date: timestamp (nullable = true)
         |-- content_name: string (nullable = true)
         |-- bitrate_rx: long (nullable = true)
         */


        Map<String, String> cfg1 = new HashedMap();
        cfg1.put("es.nodes", esNode);
        cfg1.put("es.port", esPort);
        cfg1.put("es.resource", indexVodCatchupAll);
        cfg1.put("es.spark.dataframe.write.null", "true");

        Map<String, String> cfg2 = new HashedMap();
        cfg2.put("es.nodes", esNode);
        cfg2.put("es.port", esPort);
        cfg2.put("es.resource", indexCanal);
        cfg2.put("es.spark.dataframe.write.null", "true");

        JavaEsSparkSQL.saveToEs(sqlFinalVodCatchupAllDF, cfg1);
        JavaEsSparkSQL.saveToEs(sqlFinalCanalDF, cfg2);


/*
        sqlFinalVodCatchupAllDF.printSchema();
        sqlFinalVodCatchupAllDF.write().csv("C:\\temp\\result-all.csv");

        sqlcanalDF.printSchema();
        sqlcanalDF.write().csv("C:\\temp\\result-canal.csv");
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
            files[i] = rootcsv + "/output_logstash/"+ logcomponent +"/"+ model + "/" + day + "/data.csv";
            //files[i] = rootcsv + "\\output_logstash\\"+ logcomponent +"\\"+ model + "\\" + day + "\\data.csv";
            i++;
        }
        return files;
    }
}
