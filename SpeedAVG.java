package master2017.flink;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;



public class SpeedAVG implements WindowFunction
        <Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
                Tuple6<Integer, Integer, Integer, Integer, Integer, Double>,
                Tuple, TimeWindow> {

    Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = new Tuple8<>();
    public static final int MAXSPEEDAVG = 60;
    public static final int SEGMIN = 52;
    public static final int SEGMAX = 56;

    @Override
    public void apply(Tuple tuple, TimeWindow eventTimeSessionWindows,
                      Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable,
                      Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> collector) throws Exception {

        Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = iterable.iterator();
        first = iterator.next();
        Integer time = 0;
        Integer time2 = 0;
        Integer Spd = 0;
        Integer XWay = 0;
        Integer Dir = 0;
        Integer Seg = 0;
        Integer count = 1;
        if (first != null) {
            time = first.f0;
            Spd = first.f2;
            XWay = first.f3;
            Dir = first.f5;
            Seg = first.f6;
        }
        if (Seg == SEGMIN || Seg == SEGMAX) {
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> other = null;
            while (iterator.hasNext()) {
                other = iterator.next();
                Spd += other.f2;
                count++;
            }

            if (count > 1) {
                Seg = other.f6;
                if (Seg.intValue() == SEGMAX && Dir == 0) {//Time, VID, Spd, XWay, Lane, Dir, Seg, Pos -- Time1, Time2, VID, XWay, Dir, AvgSpd,
                    double avgS = Spd / count;
                    if (avgS > MAXSPEEDAVG) {
                        time2 = other.f0;

                        collector.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>
                                (time, time2, first.f1, XWay, Dir, avgS));
                    }
                } else if (Seg.intValue() == SEGMIN && Dir == 1) {//Time, VID, Spd, XWay, Lane, Dir, Seg, Pos -- Time1, Time2, VID, XWay, Dir, AvgSpd,
                    double avgS = Spd / count;

                    if (avgS > MAXSPEEDAVG) {
                        time2 = other.f0;
                        collector.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>
                                (time, time2, first.f1, XWay, Dir, avgS));
                    }

                }
            }
        }
    }
}
