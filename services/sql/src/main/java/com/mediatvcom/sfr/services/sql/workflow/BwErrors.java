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
public class BwErrors implements Serializable {

    private final SparkSession spark;
    List<String> dateRange;

    /*
    Models used by the VoD Workflow
     */

    private final UsrmSnmpModel usrmSnmpModel;
    private final UsrmTransformedSnmpModel usrmTransformedSnmpModel;
    private final UsrmVermserverRxModel usrmVermserverRxModel;
    private final UsrmVermserverTxModel usrmVermserverTxModel;


    /*
    BW errors
     */

    Dataset<Row> df_usrm_vermserver_rx;
    Dataset<Row> df_usrm_vermserver_tx;
    Dataset<Row> df_usrmSnmpModel;


    public BwErrors(SparkSession spark, String rootCsv, List<String> daterange) {
        this.spark = spark;
        this.dateRange = daterange;


        this.usrmSnmpModel    = new UsrmSnmpModel(rootCsv);
        this.usrmTransformedSnmpModel = new UsrmTransformedSnmpModel(rootCsv);
        this.usrmVermserverRxModel = new UsrmVermserverRxModel(rootCsv);
        this.usrmVermserverTxModel  = new UsrmVermserverTxModel(rootCsv);

        this.df_usrmSnmpModel = getDframe(usrmSnmpModel.getRootCsv(), usrmSnmpModel.getLogComponent(), usrmSnmpModel.getModelName(),usrmSnmpModel.getSchema(), dateRange);
        this.df_usrm_vermserver_rx = getDframe(usrmVermserverRxModel.getRootCsv(), usrmVermserverRxModel.getLogComponent(), usrmVermserverRxModel.getModelName(), usrmVermserverRxModel.getSchema(), dateRange);
        this.df_usrm_vermserver_tx = getDframe(usrmVermserverTxModel.getRootCsv(), usrmVermserverTxModel.getLogComponent(), usrmVermserverTxModel.getModelName(), usrmVermserverTxModel.getSchema(), dateRange);

    }

     public void  runWorkflow(){

         df_usrm_vermserver_rx.createOrReplaceTempView("usrm_vermserver_rx");
         df_usrm_vermserver_tx.createOrReplaceTempView("usrm_vermserver_tx");

         SnmpTransfrom snmpTransfrom = new SnmpTransfrom(df_usrmSnmpModel);

         Dataset<Row> snmpTransformedDF = spark.createDataFrame(snmpTransfrom.getRowSnmpTransformedRDD(), usrmTransformedSnmpModel.getSchema());

         snmpTransformedDF.createOrReplaceTempView("snmp_transformed");

         Dataset<Row> sql4verrorDF = spark.sql("SELECT rx.date as date_rx, rx.ondemand_session_id as ondemand_session_id_rx, " +
                 "rx.bitrate as bitrate_rx, rx.service_group as service_group_rx, regexp_extract(rx.service_group,\"(.+?_U).*\",1) as service_group_detail_rx, rx.ip_rfgw as ip_rfgw_rx, " +
                 "rx.port_rfgw as port_rfgw_rx, rx.mode as mode_rx, tx.code_http as code_http_tx, tx.cseq as cseq_tx " +
                 "FROM usrm_vermserver_rx rx " +
                 "LEFT JOIN usrm_vermserver_tx tx " +
                 "ON rx.ondemand_session_id = tx.ondemand_session_id " +
                 "where rx.mode = \"unicast\" and tx.code = \"error\" and tx.code_http = \"453 Not Enough Bandwidth\" " ).cache();

         sql4verrorDF.createOrReplaceTempView("4verrors");

         Dataset<Row> sqlsnmpDF = spark.sql(" SELECT snmptf.date_down as date_down_4vbw, snmptf.date_up as date_up_4vbw, " +
                 "4verrors.ondemand_session_id_rx as ondemand_session_id_rx_4v_4vbw " +
                 "FROM snmp_transformed snmptf " +
                 "LEFT JOIN 4verrors " +
                 "WHERE snmptf.rfgw_id = 4verrors.service_group_detail_rx " +
                 "and unix_timestamp(4verrors.date_rx, \"yyyy-MM-dd HH:mm:ss.SSS\") >= unix_timestamp(snmptf.date_down, \"yyyy-MM-dd HH:mm:ss.SSS\" ) " +
                 "and unix_timestamp(4verrors.date_rx, \"yyyy-MM-dd HH:mm:ss.SSS\") < unix_timestamp(snmptf.date_up, \"yyyy-MM-dd HH:mm:ss.SSS\" )" ).cache();

         sqlsnmpDF.createOrReplaceTempView("snmp4v");

         Dataset<Row> sqlbwDF = spark.sql("SELECT from_utc_timestamp(from_unixtime(unix_timestamp(4verrors.date_rx, \"yyyy-MM-dd HH:mm:ss.SSS\")), 'UTC') as date, " +
                 "4verrors.bitrate_rx as bitrate_rx, " +
                 "4verrors.service_group_rx as service_group, 4verrors.ip_rfgw_rx as ip_rfgw, 4verrors.port_rfgw_rx as port_rfgw, " +
                 "4verrors.service_group_detail_rx as rfgw_id, 4verrors.code_http_tx as code_http, " +
                 "from_utc_timestamp(from_unixtime(unix_timestamp(snmp4v.date_down_4vbw, \"yyyy-MM-dd HH:mm:ss.SSS\")), 'UTC') as date_down, " +
                 "from_utc_timestamp(from_unixtime(unix_timestamp(snmp4v.date_up_4vbw, \"yyyy-MM-dd HH:mm:ss.SSS\")), 'UTC') as date_up, " +
                 "4verrors.ondemand_session_id_rx as ondemand_session_id, " +
                 "if (snmp4v.ondemand_session_id_rx_4v_4vbw = 4verrors.ondemand_session_id_rx, \"SRM Disconnected From USRM\", \"Not Enough Bandwidth\") as bw_real_error " +
                 "FROM 4verrors " +
                 "LEFT JOIN snmp4v " +
                 "ON snmp4v.ondemand_session_id_rx_4v_4vbw = 4verrors.ondemand_session_id_rx ").cache();

         sqlbwDF.createOrReplaceTempView("bw_errors");

         Dataset<Row> sqlFinalBwErrorsDF = spark.sql("SELECT " +
                 "date, " +
                 "if (bitrate_rx IS NOT NULL, bitrate_rx, \"null\") as bitrate_rx, " +
                 "if (service_group IS NOT NULL, service_group, \"null\") as service_group, " +
                 "if (ip_rfgw IS NOT NULL, ip_rfgw, \"null\") as ip_rfgw, " +
                 "if (port_rfgw IS NOT NULL, port_rfgw, \"null\") as port_rfgw,  " +
                 "if (rfgw_id IS NOT NULL, rfgw_id, \"null\") as rfgw_id,  " +
                 "if (code_http IS NOT NULL, code_http, \"null\") as code_http,  " +
                 "date_down, " +
                 "date_up, " +
                 "if (ondemand_session_id IS NOT NULL, ondemand_session_id, \"null\") as ondemand_session_id,  " +
                 "if (bw_real_error IS NOT NULL, bw_real_error, \"null\") as bw_real_error  " +
                 "FROM bw_errors ").repartition(1);

         Map<String, String> cfg = new HashedMap();
         cfg.put("es.nodes", "10.1.1.157");
         cfg.put("es.port", "9200");
         cfg.put("es.resource", "bw_errors/not_enough_bandwidth");
         cfg.put("es.spark.dataframe.write.null", "true");

         JavaEsSparkSQL.saveToEs(sqlFinalBwErrorsDF, cfg);

         sqlFinalBwErrorsDF.show();
         sqlFinalBwErrorsDF.write().csv("C:\\temp\\result-bw-errors.csv");

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
