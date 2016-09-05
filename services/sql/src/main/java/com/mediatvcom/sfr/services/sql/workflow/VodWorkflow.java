package com.mediatvcom.sfr.services.sql.workflow;

import com.fasterxml.jackson.databind.util.ArrayIterator;
import com.mediatvcom.sfr.services.sql.utils.models.srm.*;
import com.mediatvcom.sfr.services.sql.utils.models.streamer.StreamerModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmSnmpModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmTransformedSnmpModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverRxModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverTxModel;
import com.mediatvcom.sfr.services.sql.utils.udfs.*;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.*;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
//import org.elasticsearch.spark.sql.java.api.JavaEsSparkSQL;
//java.api.JavaEsSpark SQL;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Seq;
import scala.tools.asm.Type;

import static org.apache.spark.sql.functions.*;

/**
 * Created by AGOUTA on 22/08/2016.
 */
public class VodWorkflow implements Serializable {

    private final SparkSession spark;
    List<String> dateRange;

    private final SrmSessionStart4cModel srmSessionStart4cModel;
    Dataset<Row> df_srmSessionStart4cModel;
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
    private final SrmEnd7v7cModel srmEnd7v7cModel;


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
    Dataset<Row> df_usrmSnmpModel;


    public VodWorkflow(SparkSession spark, String rootCsv, List<String> daterange) {
        this.spark = spark;
        this.dateRange = daterange;

        this.srmSessionStart4cModel = new SrmSessionStart4cModel(rootCsv);
        this.df_srmSessionStart4cModel = getDframe(srmSessionStart4cModel.getRootCsv(), srmSessionStart4cModel.getLogComponent(), srmSessionStart4cModel.getModelName(), srmSessionStart4cModel.getSchema(), dateRange);

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
        this.srmEnd7v7cModel  = new SrmEnd7v7cModel(rootCsv);

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

         spark.udf().register("getContentName", new ContentNameUDF(), DataTypes.StringType);
         spark.udf().register("getCarte", new Carte(), DataTypes.StringType);
         spark.udf().register("getCarteOn4vError", new CarteOn4vError(), DataTypes.StringType);
         spark.udf().register("getContentNameOnSetupOn4vError", new ContentNameOnSetupOn4vError(), DataTypes.StringType);
         spark.udf().register("getContentNameOnSetup", new ContentNameOnSetup(), DataTypes.StringType);
         spark.udf().register("getCarteOnSetup", new CarteOnSetup(), DataTypes.StringType);


         df_streamerModel = df_streamerModel
                 .withColumn("content_name", callUDF("getContentName", df_streamerModel.col("url")));
         df_streamerModel = df_streamerModel
                 .withColumn("carteId", callUDF("getCarte", df_streamerModel.col("client_id")));
         df_streamerModel.createOrReplaceTempView("streamerModel");

         df_srmSetup1vModel = df_srmSetup1vModel
                 .withColumn("carteId", callUDF("getCarteOnSetup", df_srmSetup1vModel.col("client_id_reverse")));
         df_srmSetup1vModel = df_srmSetup1vModel
                 .withColumn("content_name", callUDF("getContentNameOnSetup", df_srmSetup1vModel.col("url")));

         df_srmSetup1vModel = df_srmSetup1vModel.withColumn("content_type", lit("vod"));
         df_srmSetup1vModel.createOrReplaceTempView("srmSetup1vModel");

         df_srmTunningStartSession4v6v4s6cModel = df_srmTunningStartSession4v6v4s6cModel
                 .withColumn("carteId", callUDF("getCarteOn4vError", df_srmTunningStartSession4v6v4s6cModel.col("x_srm_error_message"), df_srmTunningStartSession4v6v4s6cModel.col("url"), df_srmTunningStartSession4v6v4s6cModel.col("client_id")));
         df_srmTunningStartSession4v6v4s6cModel = df_srmTunningStartSession4v6v4s6cModel
                 .withColumn("content_name", callUDF("getContentNameOnSetupOn4vError", df_srmTunningStartSession4v6v4s6cModel.col("url")));
         df_srmTunningStartSession4v6v4s6cModel.createOrReplaceTempView("srmTunningStartSession4V6v6S6CModel");

         SnmpTransfrom snmpTransfrom = new SnmpTransfrom(df_usrmSnmpModel);


         System.out.println(dateRange.get(0));

         Dataset<Row> sqlDF = spark.sql("SELECT t3av.date_normal as date_normal, t3av.date_3av as date_3av," +
                 " t3av.streamer_3av as streamer_3av, if(3v.cseq IS NOT NULL, 3v.cseq, \"null\") as cseq, " +
                 "if(3v.date IS NOT NULL, 3v.date, \"2016-07-07 00:00:24.000000\") as date_3v, t3av.url_3av as url_3av " +
                 "From (" +
                 " SELECT 3av.date as date_normal, unix_timestamp(3av.date, \"yyyy-MM-dd HH:mm:ss.SSSSSS\") as date_3av, " +
                    "3av.streamer as streamer_3av, 3av.cseq as cseq_3av,  3av.url as url_3av " +
                    "FROM  streamerModel 3av " +
                    "WHERE to_date(3av.date) = to_date( " + "\"" + dateRange.get(0) + "\"" + " ) " +
                 ") t3av " +
                 "LEFT JOIN srmSessionId3v5cModel 3v " +
                 "ON t3av.cseq_3av = 3v.cseq ");


         sqlDF.printSchema();

         /*, 3v.date as date_3v
                 "From streamerModel 3av " +
                 "LEFT JOIN srmSessionId3v5cModel 3v " +
                 "ON 3av.cseq = 3v.cseq and 3av.date < 2016-07-08 ");
                 unix_timestamp(3av.date, "yyyy-MM-dd HH:mm:ss.SSSSSS").cast("timestamp")
*/



         Map<String, String> cfg = new HashedMap();
         cfg.put("es.nodes", "10.1.1.157");
         cfg.put("es.port", "9200");
         cfg.put("es.resource", "vodcatchup/all");
         cfg.put("es.spark.dataframe.write.null", "true");

         JavaEsSparkSQL.saveToEs(sqlDF, cfg);


         // , cseq should be removed since it is somtimes set to some value!
         /*
         Dataset<Row> sqlDF4v = spark.sql("SELECT MIN(date) as date_min_4v, MAX(date) as date_max_4v, " +
                 "4v.session_id as sessionId_4v, " +
                 "tsid as tsid_4v, svcid as svcid_4v, " +
                 "content_type as content_type_4v " +
                 "From srmTunningStartSession4V6v6S6CModel 4v " +
                 "Group by session_id, tsid, svcid, content_type ").cache();

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


         Dataset<Row> sqlDF3a1v = spark.sql("SELECT MAX(1vus.date) as date_1vus, 1vus.content_type as content_type_1vus, 3aus.date as date_3aus, " +
                 "3aus.cseq as cseq_3aus, 3aus.client_id as client_id_3aus, 3aus.streamer as streamer_3aus, " +
                 "3aus.content_name as content_name_3aus, 3aus.carteId as carteId_3aus " +
                 "From streamerModel 3aus " +
                 "JOIN srmSetup1vModel 1vus " +
                 "ON 1vus.content_name = 3aus.content_name and 1vus.carteId = 3aus.carteId " +
                 "and unix_timestamp(3aus.date, \"yyyy-MM-dd HH:mm:ss.SSSSSS\") >= unix_timestamp(1vus.date, \"yyyy-MM-dd HH:mm:ss.SSS\") " +
                 "GROUP BY 3aus.client_id, 3aus.streamer, 3aus.cseq, 3aus.content_name, 3aus.date, 3aus.carteId, 1vus.content_type ").cache();

         sqlDF3a1v.createOrReplaceTempView("3aus1vus");

         Dataset<Row> sqlDF3aCommon = spark.sql("SELECT MAX(common.date_rx) as date_common, 3aus.date as date_3aus, 3aus.client_id as client_id_3aus, " +
                 "3aus.ip_rfgw as ip_rfgw_3aus, 3aus.port_rfgw as port_rfgw_3aus, 3aus.content_name as content_name_3aus, " +
                 "common.bitrate_rx as bitrate_rx_common, common.service_group_rx as service_group_rx_common, common.mode_rx as mode_rx_3aus " +
                 "From streamerModel 3aus " +
                 "JOIN common " +
                 "ON common.port_rfgw_tx = 3aus.port_rfgw and common.ip_rfgw_tx = 3aus.ip_rfgw " +
                 "and ( unix_timestamp(3aus.date, \"yyyy-MM-dd HH:mm:ss.SSSSSS\") + 7200 ) >= unix_timestamp(common.date_rx, \"yyyy-MM-dd HH:mm:ss.SSS\")  " +
                 "Group by 3aus.client_id, 3aus.cseq, common.bitrate_rx, common.service_group_rx, common.mode_rx, " +
                 "3aus.content_name, 3aus.date, 3aus.ip_rfgw, 3aus.port_rfgw ").cache();

         sqlDF3aCommon.createOrReplaceTempView("3auscommon");

         Dataset<Row> sqlDF = spark.sql("SELECT 3aus1vus.date_1vus as date, NULL as date_start_video, 3aus1vus.content_type_1vus as content_type, 3av.content_name as content_name, 3av.streamer as streamer, 3auscommon.bitrate_rx_common as bitrate_rx, " +
                 "3auscommon.service_group_rx_common as service_group, 3av.port_rfgw as port_rfgw, 3av.ip_rfgw as ip_rfgw, 3v.client_id as client_id, " +
                 "3av.carteId as carteId, 7v.date as date_end, 3v.session_id as sessionId, " +
                 "3auscommon.mode_rx_3aus as mode_rx, \"200 OK\" as code_http, NULL as x_srm_error_message, NULL as ondemand_session_id " +
                 "From streamerModel 3av " +
                 "LEFT JOIN srmSessionId3v5cModel 3v " +
                 "ON 3av.cseq = 3v.cseq " +
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

         sqlDF.createOrReplaceTempView("DF1");
         //date, date_start_video, content_type, content_name, streamer, bitrate_rx, service_group, port_rfgw, ip_rfgw, client_id, carteId, date_end, sessionId, mode_rx, content_type, xxxxxx
//content_type_1v as content_type,
         Dataset<Row> sqlerrorDF = spark.sql("SELECT date_4v as date, NULL as date_start_video,  " +
                 "if (content_name_4v rlike '(.+?)controlSession.*', \"catchup\", content_type_1v) as content_type, " +
                 "content_name_4v as content_name, " +
                 "NULL as streamer, NULL as bitrate_rx, NULL as service_group, NULL as port_rfgw, NULL as ip_rfgw, NULL as client_id, " +
                 "carteId_4v as carteId, NULL as date_end, NULL as sessionId, NULL as mode_rx, " +
                 "code_http_4v as code_http, x_srm_error_message_4v as x_srm_error_message, NULL as ondemand_session_id " +
                 "FROM ( " +
                 "SELECT 1v.date as date_1v, " +
                 "4v.date as date_4v, 4v.code_http as code_http_4v, 4v.x_srm_error_message as x_srm_error_message_4v, 4v.carteId as carteId_4v, 4v.content_name as content_name_4v, 1v.content_type as content_type_1v " +
                 "FROM srmTunningStartSession4V6v6S6CModel 4v " +
                 "LEFT JOIN srmSetup1vModel 1v " +
                 "ON 1v.content_name = 4v.content_name and 1v.carteId = 4v.carteId " +
                 "and unix_timestamp(4v.date, \"yyyy-MM-dd HH:mm:ss.SSS\") >= unix_timestamp(1v.date, \"yyyy-MM-dd HH:mm:ss.SSS\" ) " +
                 "where 4v.error = \"true\" and 4v.content_name rlike '^[A-Z].*' " +
                 ") errors " +
                 "GROUP BY date_4v, code_http_4v, x_srm_error_message_4v, carteId_4v, content_name_4v, content_type_1v " +
                 "UNION ALL " +
                 "select * " +
                 "FROM DF1 ").repartition(1);

         sqlerrorDF.show();
         sqlerrorDF.write().csv("C:\\temp\\result-all-errors.csv");
*/

         sqlDF.show();
         sqlDF.write().csv("C:\\temp\\result.csv");


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
