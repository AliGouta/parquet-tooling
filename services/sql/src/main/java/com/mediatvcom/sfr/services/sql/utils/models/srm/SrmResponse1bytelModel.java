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
public class SrmResponse1bytelModel implements LogModel, Serializable {

    private final StructType schema;
    private final String modelName = "bytel_response_1";
    private final String logComponent = "srm";

    private final String rootCsv;

    public SrmResponse1bytelModel(String rootCsv) {
        schema = buildschema();
        this.rootCsv = rootCsv;
    }

    private StructType buildschema() {

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("code_http", DataTypes.StringType, true),
                DataTypes.createStructField("cseq", DataTypes.StringType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("content_type", DataTypes.StringType, true),
                DataTypes.createStructField("error", DataTypes.StringType, true),
                DataTypes.createStructField("x_srm_error_message", DataTypes.StringType, true),
                DataTypes.createStructField("ott_component", DataTypes.StringType, true));


        StructType schema = DataTypes.createStructType(new ArrayList<>(fields));

        return schema;
    }

}
