package com.mediatvcom.sfr.services.sql.utils.models.usrm;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lombok.Getter;

/**
 * Created by AGOUTA on 22/08/2016.
 */
@Getter
public class UsrmVermserverRxModel implements Serializable {

    private final StructType schema;
    private final String modelName = "usrm_vermserver_rx";
    private final String logComponent = "usrm";

    private final String date;
    private final String rootCsv;

    public UsrmVermserverRxModel(String day, String rootCsv) {
        schema = buildschema();
        this.date = day;
        this.rootCsv = rootCsv;
    }

    private StructType buildschema() {

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("url", DataTypes.StringType, true),
                DataTypes.createStructField("bitrate", DataTypes.StringType, true),
                DataTypes.createStructField("ip_srm", DataTypes.StringType, true),
                DataTypes.createStructField("service_group", DataTypes.StringType, true),
                DataTypes.createStructField("ondemand_session_id", DataTypes.StringType, true),
                DataTypes.createStructField("cseq", DataTypes.StringType, true),
                DataTypes.createStructField("mode", DataTypes.StringType, true),
                DataTypes.createStructField("session", DataTypes.StringType, true),
                DataTypes.createStructField("ip_rfgw", DataTypes.StringType, true),
                DataTypes.createStructField("port_rfgw", DataTypes.StringType, true),
                DataTypes.createStructField("ott_component", DataTypes.StringType, true));


        StructType schema = DataTypes.createStructType(new ArrayList<>(fields));

        return schema;
    }

}
