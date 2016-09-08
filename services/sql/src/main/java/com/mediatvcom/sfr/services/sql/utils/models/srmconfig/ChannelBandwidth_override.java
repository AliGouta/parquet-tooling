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
    public class ChannelBandwidth_override implements LogModel {

        private final StructType schema;
        private final String modelName = "ChannelBandwidth_override";
        private final String logComponent = "srmconfig";

        private final String rootCsv;

        public ChannelBandwidth_override(String rootCsv) {
            schema = buildschema();
            this.rootCsv = rootCsv;
        }




        private StructType buildschema() {

            List<StructField> fields = Arrays.asList(
                    DataTypes.createStructField("name", DataTypes.StringType, true),
                    DataTypes.createStructField("channelnumber", DataTypes.StringType, true),
                    DataTypes.createStructField("bps", DataTypes.StringType, true),
                    DataTypes.createStructField("definition", DataTypes.StringType, true),
                    DataTypes.createStructField("networklocation", DataTypes.StringType, true));

            StructType schema = DataTypes.createStructType(new ArrayList<>(fields));

            return schema;
        }

    }
