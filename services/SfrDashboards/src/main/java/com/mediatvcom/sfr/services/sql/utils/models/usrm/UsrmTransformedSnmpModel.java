package com.mediatvcom.sfr.services.sql.utils.models.usrm;

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
public class UsrmTransformedSnmpModel implements LogModel, Serializable {

    private final StructType schema;
    private final String modelName = "usrm_transformed_snmp";
    private final String logComponent = "usrm";

    private final String rootCsv;

    public UsrmTransformedSnmpModel(String rootCsv) {
        schema = buildschema();
        this.rootCsv = rootCsv;
    }

    private StructType buildschema() {

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("rfgw_id", DataTypes.StringType, true),
                DataTypes.createStructField("date_down", DataTypes.StringType, true),
                DataTypes.createStructField("date_up", DataTypes.StringType, true));


        StructType schema = DataTypes.createStructType(new ArrayList<>(fields));

        return schema;
    }


}