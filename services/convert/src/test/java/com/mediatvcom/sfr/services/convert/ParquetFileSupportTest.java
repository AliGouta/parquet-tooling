package com.mediatvcom.sfr.services.convert;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;


import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by AGOUTA on 21/08/2016.
 */

public class ParquetFileSupportTest {

    private static final String stest = "C:\\Users\\agouta\\Desktop\\output.tar\\output\\output_logstash\\usrm\\usrm_vermserver_rx\\2016-07-08\\data.csv";
    String rootCsv = "C:\\Users\\agouta\\Desktop\\output.tar\\output";
    String rootParquet = "C:\\Users\\agouta\\Desktop\\output.tar\\output_parquet";

    @Ignore
    public void should_contain_usrm_vermserver_rx() {

        ParquetFileSupport pq = new ParquetFileSupport(rootCsv, rootParquet, new File(stest));
        String var = pq.getSchema();
        assertThat(pq.getSchema()).isEqualTo("usrm_vermserver_rx");

    }


}