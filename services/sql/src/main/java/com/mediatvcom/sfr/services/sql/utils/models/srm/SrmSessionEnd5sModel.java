    package com.mediatvcom.sfr.services.sql.utils.models.srm;

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
    public class SrmSessionEnd5sModel implements LogModel {

        private final StructType schema;
        private final String modelName = "sdv_session-end_5s";
        private final String logComponent = "srm";

        private final String date;
        private final String rootCsv;

        public SrmSessionEnd5sModel(String day, String rootCsv) {
            schema = buildschema();
            this.date = day;
            this.rootCsv = rootCsv;
        }

        private StructType buildschema() {

            List<StructField> fields = Arrays.asList(
                    DataTypes.createStructField("date", DataTypes.StringType, true),
                    DataTypes.createStructField("cseq", DataTypes.StringType, true),
                    DataTypes.createStructField("session_id", DataTypes.StringType, true),
                    DataTypes.createStructField("ott_component", DataTypes.StringType, true));


            StructType schema = DataTypes.createStructType(new ArrayList<>(fields));

            return schema;
        }

    }
