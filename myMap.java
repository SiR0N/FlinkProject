package master2017.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;

public class myMap implements MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>{

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String value) throws Exception {
		String[] numsArray = value.split(",");

		Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> a = 
				new Tuple8<>(Integer.parseInt(numsArray[0]),Integer.parseInt(numsArray[1]),Integer.parseInt(numsArray[2]),Integer.parseInt(numsArray[3]),Integer.parseInt(numsArray[4]),Integer.parseInt(numsArray[5]),Integer.parseInt(numsArray[6]),Integer.parseInt(numsArray[7]));
		return a;
	}

	

}
