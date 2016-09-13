    package com.mediatvcom.sfr.services.sql.utils.models.srmconfig;

import com.mediatvcom.sfr.services.sql.utils.models.LogModel;
import lombok.Getter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

    /**
     * Created by AGOUTA on 22/08/2016.
     */
    @Getter
    public class ServiceGroup_override implements LogModel {

        private final StructType schema;
        private final String modelName = "ServiceGroup_override";
        private final String logComponent = "srmconfig";

        private final String rootCsv;

        public ServiceGroup_override(String rootCsv) {
            schema = buildschema();
            this.rootCsv = rootCsv;
        }




        private StructType buildschema() {

            List<StructField> fields = Arrays.asList(
                    DataTypes.createStructField("ingestnameate", DataTypes.StringType, true),
                    DataTypes.createStructField("spid", DataTypes.StringType, true),
                    DataTypes.createStructField("name", DataTypes.StringType, true),
                    DataTypes.createStructField("ermName", DataTypes.StringType, true),
                    DataTypes.createStructField("scsname", DataTypes.StringType, true),
                    DataTypes.createStructField("stsdevicegroupname", DataTypes.StringType, true),
                    DataTypes.createStructField("status", DataTypes.StringType, true),
                    DataTypes.createStructField("ngods6names", DataTypes.StringType, true),
                    DataTypes.createStructField("nodegroupname", DataTypes.StringType, true),
                    DataTypes.createStructField("maxbps", DataTypes.StringType, true),
                    DataTypes.createStructField("defaultrfdvbnetworkid", DataTypes.StringType, true),
                    DataTypes.createStructField("defaultrfsympersec", DataTypes.StringType, true),
                    DataTypes.createStructField("defaultrfinnerfec", DataTypes.StringType, true),
                    DataTypes.createStructField("defaultrfouterfec", DataTypes.StringType, true),
                    DataTypes.createStructField("defaultrfmodulation", DataTypes.StringType, true),
                    DataTypes.createStructField("qamname", DataTypes.StringType, true));


            StructType schema = DataTypes.createStructType(new ArrayList<>(fields));

            return schema;
        }

    }
