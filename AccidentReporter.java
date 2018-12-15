package master2017.flink;

import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AccidentReporter implements WindowFunction
<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
Tuple7<Integer, Integer, Integer, Integer, Integer, Integer,Integer>, Tuple, TimeWindow> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	@Override
	public void apply(Tuple key, TimeWindow window,
			Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
			Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {

        Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();

        int count = 1;
        if (first != null) {

            
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> other = new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> () ;
            while (iterator.hasNext()) {
                
                 other = iterator.next();
                 count++;

                 /*
                  * We have identified a change between this two event
                  * Even if the position is the same, there is a change in the lane
                  * 
                  * We consider to report this case because even if there is a change in the event the positions is still the same
                  * 
                  * Other: 7522 1355 0 0 3 1 6 10969 
                  * first: 7432 1355 0 0 1 1 6 10969 
                  */
                /*
                 if (!comparator(other,vr1)){
                     count++;
                	 System.out.println("Other: " + other.f0 + " " + other.f1 + " " + other.f2 + " " + other.f3 + " " + other.f4 + " " + other.f5 + " " +other.f6 + " " +other.f7 + " ");
                	 System.out.println("first: " + first.f0 + " " + first.f1 + " " + first.f2 + " " + first.f3 + " " + first.f4 + " " + first.f5 + " " +first.f6 + " " +first.f7 + " ");
                	 

                 }
                 */

             }

            if (count == 4){//Time, VID, Spd, XWay, Lane, Dir, Seg, Pos -- Time1, Time2, VID, XWay, Seg, Dir, Pos

                out.collect(new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>
                        (first.f0,other.f0, first.f1, first.f3, first.f6,first.f5, first.f7));
            }
        }
		
	}
	
	public Integer[] tupleToArray(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> t){
       //Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
        Integer id = t.f1;
        Integer sdp = t.f2;
        Integer Xway = t.f3;
        Integer lane = t.f4;
        Integer dir = t.f5;
        Integer seg = t.f6;
        Integer Pos = t.f7;


        Integer[] vr = {id, sdp, Xway, lane, dir, seg, Pos};

        return  vr;
	}
	
	
	public boolean comparator (Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> t, Integer[] vr){
	
	     
	        return (t.f1.intValue() == vr[0].intValue() &&
	                t.f2.intValue() == vr[1].intValue() &&
	                t.f3.intValue() == vr[2].intValue() &&
	                t.f4.intValue() == vr[3].intValue() &&
	                t.f5.intValue() == vr[4].intValue() &&
	                t.f6.intValue() == vr[5].intValue() &&
	                t.f7.intValue() == vr[6].intValue());
	    }

}
