package com.mediatvcom.sfr.services.sql.utils.models.srm;

import com.mediatvcom.sfr.services.sql.utils.models.LogModel;
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
public class SrmSessionStart4cModel implements LogModel, Serializable {

    private final StructType schema;
    private final String modelName = "catchup_session-start_4c";
    private final String logComponent = "srm";

    private final String date;
    private final String rootCsv;

    public SrmSessionStart4cModel(String day, String rootCsv) {
        schema = buildschema();
        this.date = day;
        this.rootCsv = rootCsv;
    }

    private StructType buildschema() {

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("vip", DataTypes.StringType, true),
                DataTypes.createStructField("content_name", DataTypes.StringType, true),
                DataTypes.createStructField("control_session", DataTypes.StringType, true),
                DataTypes.createStructField("session_type", DataTypes.StringType, true),
                DataTypes.createStructField("client_id", DataTypes.StringType, true),
                DataTypes.createStructField("content_type", DataTypes.StringType, true));


        StructType schema = DataTypes.createStructType(new ArrayList<>(fields));

        return schema;
    }

}
