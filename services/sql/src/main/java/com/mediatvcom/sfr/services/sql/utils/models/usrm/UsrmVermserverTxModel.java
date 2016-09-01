package com.mediatvcom.sfr.services.sql.utils.models.usrm;

import lombok.Getter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by AGOUTA on 22/08/2016.
 */
@Getter
public class UsrmVermserverTxModel implements Serializable {

    private final StructType schema;
    private final String modelName = "usrm_vermserver_tx";
    private final String logComponent = "usrm";

    private final String date;
    private final String rootCsv;

    public UsrmVermserverTxModel(String day, String rootCsv) {
        schema = buildschema();
        this.date = day;
        this.rootCsv = rootCsv;
    }

    private StructType buildschema() {

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("code_http", DataTypes.StringType, true),
                DataTypes.createStructField("cseq", DataTypes.StringType, true),
                DataTypes.createStructField("qam_id", DataTypes.StringType, true),
                DataTypes.createStructField("ip_rfgw", DataTypes.StringType, true),
                DataTypes.createStructField("port_rfgw", DataTypes.StringType, true),
                DataTypes.createStructField("qam_destination", DataTypes.StringType, true),
                DataTypes.createStructField("modulation", DataTypes.StringType, true),
                DataTypes.createStructField("ondemand_session_id", DataTypes.StringType, true),
                DataTypes.createStructField("session", DataTypes.StringType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("ott_component", DataTypes.StringType, true));


        StructType schema = DataTypes.createStructType(new ArrayList<>(fields));

        return schema;
    }

}
