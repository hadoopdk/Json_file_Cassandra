

import java.io.FileReader;
import java.io.IOException;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Developer: Dinesh K Rajput.
 * Expained : hadoopdk.blogspot.com
 * Dev. Date: Nov-2017
 * Cassandra version 3.5
 */

public class Csd {

	public static void main(String commandLineArgs[]) {
		
		//Please provide 4 input variable like: .jar serverIp keyspace table 'inputJsonFile_path'

		String serverIP = commandLineArgs[0];
		String keyspace = commandLineArgs[1];
		String table = commandLineArgs[2];
		String inputfile = commandLineArgs[3];
		System.out.println(serverIP);
		System.out.println(keyspace);
		System.out.println(table);
		System.out.println(inputfile);
		
    //String serverIP = "127.0.0.1";
    //String keyspace = "dc1";

    Cluster cluster = Cluster.builder().addContactPoints(serverIP).build();
    Session session = cluster.connect(keyspace);

 
    
    JSONParser parser = new JSONParser();
    
     
	try {
		//JSONArray a = (JSONArray) parser.parse(new FileReader("C:\\Users\\Dinesh Rajput\\Downloads\\events1.json"));//"/root/events.json"
		JSONArray a = (JSONArray) parser.parse(new FileReader(inputfile));//"/root/events.json"
		for (Object o : a)
	    {
	      JSONObject person = (JSONObject) o;
	      
		      String cqlStatement = "INSERT INTO "+keyspace+"."+table+" JSON '{ "
	    		+" \"title\"" +" : "+ "\""+person.get("title").toString().replace("\n", "\\n").replace("\"","\\\"").replace("'", "\\\\u0027")+"\"," 
	    		+" \"description\"" +" : "+ "\""+person.get("description").toString().replace("\n", "\\n").replace("\"","\\\"").replace("'","\\\\u0027")+"\","
	    		+" \"duration\"" +" : "+ "\""+person.get("duration").toString().replace("\n", "\\n").replace("\"","\\\"").replace("'","\\\\u0027")+"\","
	    		+" \"end date\"" +" : "+ "\""+person.get("end date").toString().replace("\n", "\\n").replace("\"","\\\"").replace("'","\\\\u0027")+"\","
	    		+" \"start date\"" +" : "+ "\""+person.get("start date").toString().replace("\n", "\\n").replace("\"","\\\"").replace("'","\\\\u0027")+"\","
	    		+" \"venue\"" +" : "+ "\""+person.get("venue").toString().replace("\n", "\\n").replace("\"","\\\"").replace("'","\\\\u0027")+"\" }';";
	      
	      //System.out.println(cqlStatement);
	      session.execute(cqlStatement);
	      
	    }
	} catch (IOException | ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
	session.close();
    cluster.close();
}
}
