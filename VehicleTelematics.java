package master2017.flink;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class VehicleTelematics {

    public static final int MAXSPEEDALLOWED = 90;


    @SuppressWarnings("serial")
    public static void main(String[] args) {
        double start = System.currentTimeMillis();
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Read file by parameter
        String inputFile = args[0];
        String outputPath = args[1];
        DataStream<String> vr = env.readTextFile(inputFile).setParallelism(1);

        // Time	(0) - Timestamp (integer) in seconds
        // VID  (1) - Integer that identifies the vehicle
        // Spd  (2) - Speed in miles per hour 0-100
        // XWay	(3) - Highway in which the position is emitted
        // Lane	(4) - Lane of the highway
        // Dir  (5) - Direction. 0 for Eastbond. 1 for Westbond
        // Seg  (6) - Segment of the highway. 0 - 99
        // Pos  (7) - Numbers of meters from the highway. 0 - 527999

        // Create the parser
        DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> parsed;

        parsed = vr.map(new myMap()).setParallelism(1);

        KeyedStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> parsedKeyed = parsed.keyBy(1);

        DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> parsedWater = parsed
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor
                        <Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                        return element.f0 * 1000;
                    }
                }).setParallelism(1);


        /////////////////////////////// Exercise 1 ///////////////////////////////
        // Creation the DataStream object to the car that overcome the 90 mph
        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> fine;

        fine = parsedKeyed.flatMap(new FlatMapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
                Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {

            @Override
            public void flatMap(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in,
                                Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {

                if (in.f2 > MAXSPEEDALLOWED) {
                    // Time, VID, Spd, XWay, Lane, Dir, Seg, Pos  ->  Time, VID, XWay, Seg, Dir, Spd

                    Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> t = new Tuple6<>(in.f0, in.f1, in.f3, in.f6, in.f5, in.f2);
                    out.collect(t);
                }

            }
        });

        fine.writeAsCsv(outputPath + "speedfines.csv", OVERWRITE, "\r\n", ",").setParallelism(1);


        /////////////////////////////// Exercise 2 ///////////////////////////////

        KeyedStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> key2 = parsedWater.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {

            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> input) throws Exception {

                return (input.f6 >= 52 && input.f6 <= 56);
            }
        }).setParallelism(1).keyBy(1, 3, 5);

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> avg = key2
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .apply(new SpeedAVG());


        avg.writeAsCsv(outputPath + "avgspeedfines.csv", OVERWRITE, "\r\n", ",").setParallelism(1);


        /////////////////////////////// Exercise 3 ///////////////////////////////


        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> keyedStream = parsedWater.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {

            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> input) throws Exception {

                return (input.f2 == 0);
            }
        }).setParallelism(1).keyBy(1, 5, 7).window(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(30))).apply(new AccidentReporter());

        keyedStream.writeAsCsv(outputPath + "accidents.csv", OVERWRITE, "\r\n", ",").setParallelism(1);


        try {
            env.execute("Group 8 Assignment");
        } catch (Exception e) {
            e.printStackTrace();
        }
        double end = System.currentTimeMillis();
        System.out.println("Tiempo " + (end - start) / 1000);


    }
}

/*
Processing time: Processing time refers to the system time of the machine
that is executing the respective operation.
When a streaming program runs on processing time, all time-based operations (like time windows)
 will use the system clock of the machines that run the respective operator.

Event time: Event time is the time that each individual event occurred on its producing device.
This time is typically embedded within the records before they enter Flink and that event timestamp
 can be extracted from the record.

Ingestion time: Ingestion time is the time that events enter Flink.
At the source operator each record gets the source’s current time as a timestamp,
 and time-based operations (like time windows) refer to that timestamp.
Ingestion time sits conceptually in between event time and processing time. Compared to processing time, it is slightly more expensive, but gives more predictable results.


