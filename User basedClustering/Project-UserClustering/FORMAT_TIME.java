import java.util.StringTokenizer;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class FORMAT_TIME extends EvalFunc<String> {
	@Override
	public String exec(Tuple input){
		try{
			if (input == null || input.size() == 0)
				return null;
			String hour = (String) input.get(0);
			String time = "";
			int count=0;	
			if (hour.contains(":"))
			{
				if(hour.compareTo("00:00:00")>=0 && hour.compareTo("05:59:59")<=0){
					time = "1";
				}
				else if(hour.compareTo("06:00:00")>=0 && hour.compareTo("11:59:59")<=0){
					time = "2";
				}
				else if(hour.compareTo("12:00:00")>=0 && hour.compareTo("19:59:59")<=0){
					time = "3";
				}
				else if(hour.compareTo("20:00:00")>=0 && hour.compareTo("23:59:59")<=0){
					time = "4";
				}
			}
			else {
				time = "";
			}
			return time;
		}
		catch (ExecException ex) {
			System.out.println("Error: " + ex.toString());
		}
		return null;	
	}

}
