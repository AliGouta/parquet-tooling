package com.mediatvcom.sfr.services.sql.workflow;

import lombok.Getter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by AGOUTA on 01/09/2016.
 */
@Getter
public class SnmpTransfrom implements Serializable {

    JavaRDD<Row> rowSnmpTransformedRDD;




    public SnmpTransfrom(Dataset<Row> df){
        rowSnmpTransformedRDD = builRDD(df);
    }

    private JavaRDD<Row> builRDD(Dataset<Row> df) {

        return df.javaRDD().groupBy(getGroups())
                .mapPartitions(transformGroups())
                .map(getTransformedRows());
    }

    private Function<Tuple3, Row> getTransformedRows() {
        return new Function<Tuple3, Row>() {

    @Override
    public Row call(Tuple3 tuple3) throws Exception {
        return RowFactory.create(tuple3._1(), tuple3._2(), tuple3._3());
    }
};
    }

    private FlatMapFunction<Iterator<Tuple2<String, Iterable<Row>>>, Tuple3> transformGroups() {
        return new FlatMapFunction<Iterator<Tuple2<String,Iterable<Row>>>, Tuple3>() {

            @Override
            public Iterator<Tuple3> call(Iterator<Tuple2<String, Iterable<Row>>> grpRows) throws Exception {

                //Iterator<Tuple3> ret = new ArrayListIterator();
                ArrayList<Tuple3> ret = new ArrayList<Tuple3>();

                while (grpRows.hasNext()){
                    Tuple2<String, Iterable<Row>> iter = grpRows.next();
                    Iterator<Row> grpsRows = iter._2.iterator();
                    String key = iter._1;

                    while (grpsRows.hasNext()){
                        Row iterRow = grpsRows.next();
                        if (iterRow.get(2).equals("Alarm")){
                            if (grpsRows.hasNext()) {
                                String alarmDate = (String) iterRow.get(0);
                                iterRow = grpsRows.next();
                                if (iterRow.get(2).equals("OK")){
                                    ret.add(new Tuple3(key, alarmDate, iterRow.get(0)));
                                }
                            }
                        }
                    }

                }
                return  ret.iterator();
            }
        };
    }

    private Function<Row, String> getGroups() {
        return new Function<Row, String>() {

            @Override
            public String call(Row row) throws Exception {
                return (String)row.get(1);
            }
        };
    }


}
