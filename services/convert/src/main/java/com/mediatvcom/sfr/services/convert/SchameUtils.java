package com.mediatvcom.sfr.services.convert;

/**
 * Created by AGOUTA on 21/08/2016.
 */

public class SchameUtils {

    public static String catchup_get_0c_schema = "";
    public static String catchup_post_1c_schema = "";
    public static String catchup_session_start_4c_schema = "";
    public static String catchup_end_8c_schema = "";

    public static String sdv_session_start_1s_schema = "";
    public static String sdv_session_end_5s_schema = "";

    public static String vod_setup_1v_schema = "";
    public static String vod_catchup_ressource_2c_2v_schema = "";
    public static String vod_catchup_sessionId_3v_5c_schema = "";
    public static String vod_catchup_tuning_start_session_4v_6v_4s_6c_schema = "";
    public static String vod_session_start_5v_schema = "";
    public static String vod_catchup_end_7v_7c_schema = "";

    public static String USRM_SNMP_SCHEMA = "message usrm_snmp_schema{required binary date;"+
            "required binary object;" +
            "required binary state;}";

    public static String USRM_VERMSERVER_RX_SCHEMA = "message usrm_vermserver_rx{required binary date;"+
            "required binary url;" +
            "required binary bitrate;"+
            "required binary ip_srm;" +
            "required binary service_group;"+
            "required binary ondemand_session_id;" +
            "required binary cseq;"+
            "required binary mode;" +
            "required binary session;"+
            "required binary ip_rfgw;" +
            "required binary port_rfgw;}";

    public static String USRM_VERMSERVER_TX_SCHEMA = "message usrm_vermserver_tx{required binary date;"+
            "required binary code_http;" +
            "required binary cseq;"+
            "required binary qam_id;" +
            "required binary ip_rfgw;"+
            "required binary port_rfgw;" +
            "required binary qam_destination;"+
            "required binary modulation;" +
            "required binary ondemand_session_id;"+
            "required binary session;" +
            "required binary code;}";



}