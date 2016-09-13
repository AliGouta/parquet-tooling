package com.mediatvcom.sfr.services.sql.workflow;

import com.mediatvcom.sfr.services.sql.utils.models.srm.SrmSessionStart1sModel;
import com.mediatvcom.sfr.services.sql.utils.models.srmconfig.ChannelBandwidth_override;
import com.mediatvcom.sfr.services.sql.utils.models.srmconfig.ServiceGroup_override;
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


/**
 * Created by AGOUTA on 22/08/2016.
 */
public class SDV implements Serializable {

    private final SparkSession spark;
    List<String> dateRange;

    private final String esNode;
    private final String esPort;
    private final String indexSdv;

    /*
    Models used by the VoD Workflow
     */

    private final SrmSessionStart1sModel srmSessionStart1sModel;
    private final UsrmVermserverRxModel usrmVermserverRxModel;
    private final UsrmVermserverTxModel usrmVermserverTxModel;

    private final ChannelBandwidth_override channelBandwidth_override;
    private final ServiceGroup_override serviceGroup_override;

    Dataset<Row> df_srmSessionStart1sModel;
    Dataset<Row> df_usrm_vermserver_rx;
    Dataset<Row> df_usrm_vermserver_tx;

    Dataset<Row> df_channelBandwidth_override;
    Dataset<Row> df_serviceGroup_override;

    /*
    DataFrames for VoD
     */


    public SDV(SparkSession spark, String rootCsv, List<String> daterange, String esnode, String esport, String indexsdv) {
        this.spark = spark;
        this.dateRange = daterange;

        this.esNode = esnode;
        this.esPort = esport;
        this.indexSdv = indexsdv;

        this.srmSessionStart1sModel = new SrmSessionStart1sModel(rootCsv);
        this.usrmVermserverRxModel = new UsrmVermserverRxModel(rootCsv);
        this.usrmVermserverTxModel  = new UsrmVermserverTxModel(rootCsv);

        this.channelBandwidth_override = new ChannelBandwidth_override(rootCsv);
        this.serviceGroup_override = new ServiceGroup_override(rootCsv);

        this.df_srmSessionStart1sModel = getDframe(srmSessionStart1sModel.getRootCsv(), srmSessionStart1sModel.getLogComponent(), srmSessionStart1sModel.getModelName(), srmSessionStart1sModel.getSchema(), dateRange);
        this.df_usrm_vermserver_rx = getDframe(usrmVermserverRxModel.getRootCsv(), usrmVermserverRxModel.getLogComponent(), usrmVermserverRxModel.getModelName(), usrmVermserverRxModel.getSchema(), dateRange);
        this.df_usrm_vermserver_tx = getDframe(usrmVermserverTxModel.getRootCsv(), usrmVermserverTxModel.getLogComponent(), usrmVermserverTxModel.getModelName(), usrmVermserverTxModel.getSchema(), dateRange);
        this.df_channelBandwidth_override = getDframe(channelBandwidth_override.getRootCsv(), channelBandwidth_override.getLogComponent(), channelBandwidth_override.getModelName(), channelBandwidth_override.getSchema(), dateRange);
        this.df_serviceGroup_override = getDframe(serviceGroup_override.getRootCsv(), serviceGroup_override.getLogComponent(), serviceGroup_override.getModelName(), serviceGroup_override.getSchema(), dateRange);


    }

     public void  runWorkflow(){


         df_usrm_vermserver_rx.createOrReplaceTempView("usrm_vermserver_rx");
         df_usrm_vermserver_tx.createOrReplaceTempView("usrm_vermserver_tx");

         df_serviceGroup_override.createOrReplaceTempView("service_group_override");
         df_channelBandwidth_override.createOrReplaceTempView("channel_bandwidth_override");

         spark.udf().register("getClientCarte", new ClientCarteSdv(), DataTypes.StringType);
         spark.udf().register("getClientNit", new ClientNitSdv(), DataTypes.StringType);
         spark.udf().register("getClientSG", new ClientServiceGroupSdv(), DataTypes.StringType);
         spark.udf().register("getTimeMilli", new SdvTriggerSessionPrecision(), DataTypes.DoubleType);

         df_srmSessionStart1sModel = df_srmSessionStart1sModel.withColumn("carte_id", callUDF("getClientCarte", df_srmSessionStart1sModel.col("client_id")));
         df_srmSessionStart1sModel = df_srmSessionStart1sModel.withColumn("nit_id", callUDF("getClientNit", df_srmSessionStart1sModel.col("client_id")));
         df_srmSessionStart1sModel = df_srmSessionStart1sModel.withColumn("service_group_id", callUDF("getClientSG", df_srmSessionStart1sModel.col("client_id")));

         df_srmSessionStart1sModel.createOrReplaceTempView("setup1s");

         Dataset<Row> sqlcommonDF = spark.sql("SELECT rx.date as date_rx, " +
                 "unix_timestamp(rx.date, \"yyyy-MM-dd HH:mm:ss.SSS\") as date_rx_seconds, " +
                 "regexp_extract(rx.date, \".+?(.)([0-9]{3})\", 2) as date_rx_micro, " +
                 "tx.date as date_tx, endrx.date as date_endrx, " +
                 "rx.mode as mode_rx, endrx.mode as mode_endrx, rx.ondemand_session_id as ondemand_session_id_rx, rx.bitrate as bitrate_rx, " +
                 "rx.service_group as service_group_rx, rx.ip_rfgw as ip_rfgw_rx, rx.port_rfgw as port_rfgw_rx, tx.session as session_tx, " +
                 "tx.qam_id as qam_id_tx, tx.qam_destination as qam_destination_tx," +
                 "ch.channelnumber as channelnumber " +
                 "FROM usrm_vermserver_rx rx " +
                 "JOIN usrm_vermserver_tx tx " +
                 "ON rx.ondemand_session_id = tx.ondemand_session_id and rx.cseq = tx.cseq and rx.mode = \"multicast\" and tx.code = \"reservation\" " +
                 "LEFT JOIN usrm_vermserver_rx endrx " +
                 "ON endrx.ondemand_session_id = rx.ondemand_session_id and endrx.mode = \"teardown\" " +
                 "JOIN channel_bandwidth_override ch " +
                 "ON CONCAT(rx.ip_rfgw, ':', rx.port_rfgw) = ch.networklocation ").cache();

         sqlcommonDF = sqlcommonDF.withColumn("data_rx_milli", callUDF("getTimeMilli", sqlcommonDF.col("date_rx_seconds"), sqlcommonDF.col("date_rx_micro")));
         sqlcommonDF.createOrReplaceTempView("common");


         Dataset<Row> sqlchannelDF = spark.sql("SELECT tb1s.date as date_channel, " +
                 "unix_timestamp(tb1s.date, \"yyyy-MM-dd HH:mm:ss.SSS\") as date_channel_seconds, " +
                 "regexp_extract(tb1s.date, \".+?(.)([0-9]{3})\", 2) as date_channel_micro, " +
                 "sg.qamname as qamname, CONCAT(sg.qamname, '.', tb1s.service_group_id) as service_group, tb1s.sdv as sdv, " +
                 "tb1s.carte_id as carte_id, tb1s.nit_id as nit_id, tb1s.service_group_id as service_group_id, " +
                 "ch.name as channel_name, ch.bps " +
                 "FROM ( " +
                    "SELECT date, service_group_id, sdv, carte_id, nit_id, service_group_id " +
                    "FROM setup1s trtb1s " +
                    "WHERE to_date(trtb1s.date) = to_date( " + "\"" + dateRange.get(0) + "\"" + " )" +
                 ") tb1s " +
                 "JOIN service_group_override sg " +
                 "ON CONCAT(tb1s.nit_id, '.', tb1s.service_group_id) = sg.ingestnameate " +
                 "JOIN channel_bandwidth_override ch " +
                 "ON tb1s.sdv = ch.channelnumber ").cache();

         sqlchannelDF = sqlchannelDF.withColumn("data_channel_milli", callUDF("getTimeMilli", sqlchannelDF.col("date_channel_seconds"), sqlchannelDF.col("date_channel_micro")));
         sqlchannelDF.createOrReplaceTempView("channel");


         Dataset<Row> sqldateleaderDF = spark.sql("SELECT common.date_rx as date_rx, common.date_endrx  as date_endrx, common.ondemand_session_id_rx, " +
                 "common.service_group_rx as service_group, common.channelnumber as channelnumber, MAX(channel.date_channel) as date_leader " +
                 "FROM common " +
                 "LEFT JOIN channel " +
                 "ON channel.service_group = common.service_group_rx and channel.sdv = common.channelnumber " +
                 "WHERE ( (common.data_rx_milli + 1000) >=  (channel.data_channel_milli + 7200 * 1000) ) " +
                 "AND ( ( (common.data_rx_milli + 1000) - (channel.data_channel_milli + 7200 * 1000) ) < 10000 ) " +
                 "GROUP BY common.date_rx, common.service_group_rx, common.channelnumber, common.date_endrx, common.ondemand_session_id_rx ").cache();

         sqldateleaderDF.createOrReplaceTempView("dateleader");


         Dataset<Row> sqlleaderDF = spark.sql(" SELECT dateleader.date_rx as date_rx, dateleader.date_endrx as date_endrx, dateleader.date_leader as date_leader, " +
                 "dateleader.service_group as service_group, dateleader.channelnumber as channelnumber, channel.carte_id as carte_leader " +
                 "FROM dateleader " +
                 "JOIN channel " +
                 "ON channel.date_channel = dateleader.date_leader and channel.service_group = dateleader.service_group and channel.sdv = dateleader.channelnumber ").cache();

         sqlleaderDF.createOrReplaceTempView("leader");


         Dataset<Row> sqlfollowerDF = spark.sql("SELECT from_utc_timestamp(from_unixtime(unix_timestamp(leader.date_rx, \"yyyy-MM-dd HH:mm:ss.SSS\")), 'UTC') as date_start_reservation, " +
                 "from_utc_timestamp(from_unixtime(unix_timestamp(leader.date_endrx, \"yyyy-MM-dd HH:mm:ss.SSS\")), 'UTC') as date_end_reservation, " +
                 "leader.date_leader date_utc_trigger_reservation, " +
                 "channel.qamname as rfgw, leader.channelnumber as channel_number, leader.service_group as service_group, leader.carte_leader as carte_leader, " +
                 "channel.carte_id as carte_id, channel.channel_name as channel_name " +
                 "FROM leader " +
                 "JOIN channel " +
                 "ON channel.sdv = leader.channelnumber and leader.service_group = channel.service_group and " +
                 "( unix_timestamp(channel.date_channel, \"yyyy-MM-dd HH:mm:ss.SSSSSS\") + 7200 ) >= unix_timestamp(leader.date_rx, \"yyyy-MM-dd HH:mm:ss.SSS\") and " +
                 "( unix_timestamp(channel.date_channel, \"yyyy-MM-dd HH:mm:ss.SSSSSS\") + 7200 ) < unix_timestamp(leader.date_endrx, \"yyyy-MM-dd HH:mm:ss.SSS\") ").cache();

         sqlfollowerDF.createOrReplaceTempView("sdv_final");

         Dataset<Row> sqlFinalSdvDF = spark.sql("SELECT " +
                 "date_start_reservation, " +
                 "date_end_reservation, " +
                 "if (date_utc_trigger_reservation IS NOT NULL, date_utc_trigger_reservation, \"null\") as date_utc_trigger_reservation, " +
                 "if (rfgw IS NOT NULL, rfgw, \"null\") as rfgw, " +
                 "if (channel_number IS NOT NULL, channel_number, 0) as channel_number, " +
                 "if (service_group IS NOT NULL, service_group, \"null\") as service_group,  " +
                 "if (carte_leader IS NOT NULL, carte_leader, \"null\") as carte_leader,  " +
                 "if (carte_id IS NOT NULL, carte_id, \"null\") as carte_id, " +
                 "if (channel_name IS NOT NULL, channel_name, \"null\") as channel_name " +
                 "FROM sdv_final ").repartition(1);

         /*
        root
         |-- date_start_reservation: timestamp (nullable = true)
         |-- date_end_reservation: timestamp (nullable = true)
         |-- date_utc_trigger_reservation: string (nullable = true)
         |-- rfgw: string (nullable = true)
         |-- channel_number: string (nullable = true)
         |-- service_group: string (nullable = true)
         |-- carte_leader: string (nullable = true)
         |-- carte_id: string (nullable = true)
          */

         sqlFinalSdvDF.printSchema();

         Map<String, String> cfg = new HashedMap();
         cfg.put("es.nodes", esNode);
         cfg.put("es.port", esPort);
         cfg.put("es.resource", indexSdv);
         cfg.put("es.spark.dataframe.write.null", "true");

         JavaEsSparkSQL.saveToEs(sqlFinalSdvDF, cfg);


         /*
         sqlFinalSdvDF.show();
         sqlFinalSdvDF.write().csv("C:\\temp\\result-sdv.csv");
         */


     }

    private Dataset<Row> getDframe(String rootcsv, String logcomponent, String model, StructType schema, List<String> daterange) {

        String[] files ;
        
        if (model.equals("ChannelBandwidth_override") || model.equals("ServiceGroup_override")){
            files = new String[1];
            files[0] = rootcsv + "/output_logstash/"+ logcomponent +"/"+ model + "/data.csv";
            //files[0] = rootcsv + "\\output_logstash\\"+ logcomponent +"\\"+ model + "\\data.csv";
        }
        else{
            files = getFilePaths(rootcsv, logcomponent, model, daterange);
        }

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
