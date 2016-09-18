package src.Couch_Code;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Document;
import com.fourspaces.couchdb.Session;


public class Couch_Client {

	private Session Server_array[];
	private Database db[];
	private Logger log;
	private Properties property_val;
	private String[] key_val;
	private int instances;
	private int operation_count;
	private String database_name;
	
	public void initializeServer() {
		instances = Integer.parseInt(property_val.getProperty("noOfServers"));
		Server_array = new Session[instances];
		db = new Database[instances];
		database_name = key_val[0];
		for (int i = 0; i < instances; i++) {
			Server_array[i] = new Session(property_val.getProperty("serverIp" + (i + 1)),5984);
			Server_array[i].createDatabase(database_name);
			db[i] = Server_array[i].getDatabase(database_name);
		}
	}

	public void put() {
		
		long startTime = System.currentTimeMillis();
		int operation_count = Integer.parseInt(property_val.getProperty("operation_count"));
		log.info("Putting Process Started At " + startTime);
		for (int i = 0; i < operation_count; i++) {
			int serverNumber = getHashKey(key_val[i]);
			Document document = new Document();
			document.setId(key_val[i]);
	        document.put("value", value);
	        db[serverNumber].saveDocument(document);
		}
		long endTime = System.currentTimeMillis();
		log.info("Total time taken to Insert " + operation_count + " operations on Couch_DB Server :"
				+ (endTime - startTime) + " milliseconds\n");
	}

	public void get() {
		long startTime = System.currentTimeMillis();
		log.info("Getting Process Started At " + startTime);
		for (int i = 0; i < operation_count; i++) {
			int serverNumber = getHashKey(key_val[i]);
			Document document = db[serverNumber].getDocument(key_val[i]);
			String string= (String)document.get("value");
			System.out.println(string);
		}
		long endTime = System.currentTimeMillis();
		log.info("Total time taken to Lookup " + operation_count + " operations on Couch_DB Server :"
				+ (endTime - startTime) + " milliseconds\n");
	}

	public void delete() {
		long startTime = System.currentTimeMillis();
		log.info("Deleting Process Started At " + startTime);
		for (int i = 0; i < operation_count; i++) {
			int serverNumber = getHashKey(key_val[i]);
			Document document = db[serverNumber].getDocument(key_val[i]);
			db[serverNumber].deleteDocument(document);
		}
		long endTime = System.currentTimeMillis();
		log.info("Total time taken to Remove " + operation_count + " operations on Couch_DB Server :"
				+ (endTime - startTime) + " milliseconds\n");
	}

	private int getHashKey(String key) {
		int hashValue = 0;
		char[] myCharArray = new char[key.length()];
		key.getChars(0, key.length(), myCharArray, 0);
		for (char myChar : myCharArray) {
			hashValue += (int) myChar;
		}
		return (hashValue * 17) % instances;
	}

	/**
	 * This method creates random key_val of length 10 bytes.
	 */
	public void createRandomKeys() {
		property_val = new Properties();
		try {
			property_val.load(new FileInputStream(new File("./resources/config.properties")));
			operation_count = Integer.parseInt(property_val.getProperty("operation_count"));
			key_val = new String[operation_count];
			char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
			Random random = new Random();
			for (int i = 0; i < operation_count; i++) {
				StringBuilder sb = new StringBuilder();
				for (int j = 0; j < 10; j++) {
					char c = chars[random.nextInt(chars.length)];
					sb.append(c);
				}
				key_val[i] = sb.toString();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void initializeLogger() {
		log = Logger.getLogger(Couch_Client.class.getName());
		try {
			FileHandler filehandler = new FileHandler();
			filehandler = new FileHandler("./resources/MyLogFile.log", true);
			log.addHandler(filehandler);
			SimpleFormatter formatter = new SimpleFormatter();
			filehandler.setFormatter(formatter);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
