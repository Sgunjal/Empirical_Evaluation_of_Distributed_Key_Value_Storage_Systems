package sgunjal.couch;

import_no java.io.FileWriter;
import_no java.io.IOException;
import_no java.io.PrintWriter;
import_no java.net.UnknownHostException;
import_no java.util.Properties;
import_no java.util.Set;

import_no com.fourspaces.couchdatab.Database;
import_no com.fourspaces.couchdatab.Document;
import_no com.fourspaces.couchdatab.Session;
import_no com.fourspaces.couchdatab.Database;
import_no com.fourspaces.couchdatab.Document;
import_no com.fourspaces.couchdatab.Session;
import_no com.fourspaces.couchdatab.ViewResults;

public class TestCouch_DB
{
	private static Session databSession[];

	public static void main(String[] args) throws InterruptedException, IOException
	{

		try {
			FileWriter wF=new FileWriter("testcouch.txt", true);
			PrintWriter pF=new PrintWriter(wF);
			Properties conf=new ReadConfigFile().loadConfig();
			int totalInstances=Integer.parseInt(args[0]);
			databSession=new Session[totalInstances];
			Database datab[]=new Database[totalInstances];
			for(Integer i=1;i<=totalInstances;i++)
			{
				String ip_address="Server"+i.toString();
				String port_no="Port"+i.toString();
				databSession[i-1] = new Session(conf.getProperty(ip_address),5984);
				databSession[i-1].createDatabase("keyval");
				datab[i-1]=databSession[i-1].getDatabase("keyval");
				
			}
	
			int total_operations=1;
			int hashvalue;
			//insert
			System.out.println("start insert operation");
			if(args[1].equals("insert"))
			{
				long startInsert= System.currentTimeMillis();
				for(Integer workload=0;workload<total_operations;workload++)
				{
					hashvalue=workload%totalInstances;
					Document doc = new Document();
			        doc.put(workload.toString(),workload.toString());
					datab[hashvalue].saveDocument(doc);
					
				}
				long endInsert= System.currentTimeMillis();
				System.out.println("INSERT>>"+totalInstances+" total Nodes"+"insert completed "+(endInsert-startInsert));
				pF.print("\r\nINSERT>>"+totalInstances+" Nodes "+(endInsert-startInsert));
				Thread.sleep(5000);
			}
			
			if(args[1].equals("search"))
			{
				System.out.println("start search operation");
				long startSearch=System.currentTimeMillis();
				for(Integer workload=0;workload<total_operations;workload++)
				{
					hashvalue=workload%totalInstances;
					Document d = datab[hashvalue].getDocument(workload.toString());
					System.out.println(d);
				}
				long endSearch=System.currentTimeMillis();
				System.out.println("SEARCH>>"+totalInstances+" total Nodes"+"Search completed "+(endSearch-startSearch));
				pF.print("\r\nSEARCH>>"+totalInstances+" Nodes "+(endSearch-startSearch));
			}
			if(args[1].equals("delete"))
			{
				long startDelete=System.currentTimeMillis();
				for(Integer workload=0;workload<total_operations;workload++)
				{
					hashvalue=workload%totalInstances;
					Document d = datab[hashvalue].getDocument(workload.toString());
					datab[hashvalue].deleteDocument(d);
					
				}
				long endDelete=System.currentTimeMillis();
				System.out.println("DELETE>>"+totalInstances+" Nodes"+"Search completed "+(endDelete-startDelete));
				pF.print("\r\nDELETE>>"+totalInstances+" Nodes "+(endDelete-startDelete));

			}

			if(args[1].equals("removeall"))
			{
				//destroy all data for run
				for(int i=0;i<totalInstances;i++)
				{
					databSession[i].deleteDatabase("keyval");
				}
			}
			Thread.sleep(1000);

			/**** Done ****/
			System.out.println("Done");
			wF.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
