package com.mediatvcom.sfr.services.sql.utils.models.streamer;

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
public class StreamerModel implements LogModel, Serializable {

    private final StructType schema;
    private final String modelName = "setupPlayer";
    private final String logComponent = "streamer";

    private final String date;
    private final String rootCsv;

    public StreamerModel(String day, String rootCsv) {
        schema = buildschema();
        this.date = day;
        this.rootCsv = rootCsv;
    }

    private StructType buildschema() {

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("url", DataTypes.StringType, true),
                DataTypes.createStructField("client_id", DataTypes.StringType, true),
                DataTypes.createStructField("ip_rfgw", DataTypes.StringType, true),
                DataTypes.createStructField("port_rfgw", DataTypes.StringType, true),
                DataTypes.createStructField("streamer", DataTypes.StringType, true),
                DataTypes.createStructField("cseq", DataTypes.StringType, true));


        StructType schema = DataTypes.createStructType(new ArrayList<>(fields));

        return schema;
    }


}