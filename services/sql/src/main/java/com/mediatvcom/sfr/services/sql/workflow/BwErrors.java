package com.mediatvcom.sfr.services.sql.workflow;

import com.mediatvcom.sfr.services.sql.utils.models.srm.*;
import com.mediatvcom.sfr.services.sql.utils.models.streamer.StreamerModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmSnmpModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmTransformedSnmpModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverRxModel;
import com.mediatvcom.sfr.services.sql.utils.models.usrm.UsrmVermserverTxModel;
import com.mediatvcom.sfr.services.sql.utils.udfs.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.lit;

/**
 * Created by AGOUTA on 22/08/2016.
 */
public class BwErrors implements Serializable {

    private final SparkSession spark;
    private String rootCsv;
    private String day;

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


    public BwErrors(SparkSession spark, String rootCsv, String day) {
        this.spark = spark;
        this.rootCsv = rootCsv;
        this.day = day;

        this.usrmSnmpModel    = new UsrmSnmpModel(day, rootCsv);
        this.usrmTransformedSnmpModel = new UsrmTransformedSnmpModel(day, rootCsv);
        this.usrmVermserverRxModel = new UsrmVermserverRxModel(day, rootCsv);
        this.usrmVermserverTxModel  = new UsrmVermserverTxModel(day, rootCsv);

        this.df_usrmSnmpModel = getDframe(usrmSnmpModel.getRootCsv(), usrmSnmpModel.getLogComponent(), usrmSnmpModel.getModelName(),usrmSnmpModel.getSchema(), day);
        this.df_usrm_vermserver_rx = getDframe(usrmVermserverRxModel.getRootCsv(), usrmVermserverRxModel.getLogComponent(), usrmVermserverRxModel.getModelName(), usrmVermserverRxModel.getSchema(), day);
        this.df_usrm_vermserver_tx = getDframe(usrmVermserverTxModel.getRootCsv(), usrmVermserverTxModel.getLogComponent(), usrmVermserverTxModel.getModelName(), usrmVermserverTxModel.getSchema(), day);

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

         Dataset<Row> sqlbwDF = spark.sql("SELECT 4verrors.date_rx as date, 4verrors.bitrate_rx as bitrate_rx, " +
                 "4verrors.service_group_rx as service_group, 4verrors.ip_rfgw_rx as ip_rfgw, 4verrors.port_rfgw_rx as port_rfgw, " +
                 "4verrors.service_group_detail_rx as service_group, 4verrors.code_http_tx as code_http, " +
                 "snmp4v.date_down_4vbw as date_down, snmp4v.date_up_4vbw as date_up, 4verrors.ondemand_session_id_rx as ondemand_session_id, " +
                 "if (snmp4v.ondemand_session_id_rx_4v_4vbw = 4verrors.ondemand_session_id_rx, \"SRM Disconnected From USRM\", \"Not Enough Bandwidth\") as bw_real_error " +
                 "FROM 4verrors " +
                 "LEFT JOIN snmp4v " +
                 "ON snmp4v.ondemand_session_id_rx_4v_4vbw = 4verrors.ondemand_session_id_rx ").repartition(1);

         sqlbwDF.show();
         sqlbwDF.write().csv("C:\\temp\\result-bw-errors.csv");

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
