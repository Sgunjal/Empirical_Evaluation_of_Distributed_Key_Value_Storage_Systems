import java.net.*;
import java.io.*;
import java.util.*;
import java.sql.Timestamp;

public class Client_Test_P extends Thread
{
	String type;
	Socket incoming_socket;
	Socket[] s2_array=new Socket[8];
	BufferedReader[] s_input2_array=new BufferedReader[8];
	PrintWriter[] s_out2_array=new PrintWriter[8];
	//InetAddress[] ia1;
	static Hashtable<String, String> list=new Hashtable<String, String>();
	static String []ip =new String[10];
	static String []replica_ip =new String[10];
	static int []port =new int[10];
	static int []replica_port =new int[10];
	static char charArray[]={'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};
	int start_counter=1;
	int end_counter=100000;
	int port1=25003;
	int thread_start=0;
	int thread_end=3;
	static int instance=4;
	int keysize=100010;
	String i_key[]=new String[keysize];
	String i_val[]=new String[keysize];
	int k=0;
	Client_Test_P(String type)
	{
		this.type=type;
	}
	Client_Test_P(Socket s,String type)
	{
		this.type=type;
		incoming_socket=s;
	}
	synchronized public void run() 
	{		
		try
		{
			if(type.equals("Server"))
			{
				ServerSocket ss=new ServerSocket(port1);			
				type="Request";
				while(true)
				{
					 Socket s = ss.accept();  		
					 Thread t1 =new Client_Test_P(s,"Request");		
					 t1.start();
				}				
			}
			else
			{				
				BufferedReader sc_input=new BufferedReader(new InputStreamReader(System.in));     
				BufferedReader s_input=new BufferedReader(new InputStreamReader(incoming_socket.getInputStream())); 
				PrintWriter s_out=new PrintWriter(incoming_socket.getOutputStream(),true);          
			//	Hashtable list=new Hashtable(50); 
				Server_request(sc_input, s_input, s_out);
		
			}
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
	synchronized public void Server_request(BufferedReader sc_input,BufferedReader s_input,PrintWriter s_out) throws Exception
	{
		String user_choice,key1,value1,get1,delete1,str,xxxx;
		Enumeration names; 
		int choice;		
		try
		{						     				
			choice=Integer.parseInt(s_input.readLine());	
		//	s_out.println("done");						
			switch(choice)
			{
				case 1: 
						while(!((key1=s_input.readLine()).equals("Exit")))
						{
						//	s_out.println("done");
							value1=s_input.readLine();																								
						//	s_out.println("done");
							list.put(key1,value1);
						//	s_out.println("done");
					//		s_out.flush();
						}
						//incoming_socket.close();						
					//	s_out.println("Value stored sucessfully !!!");
						Server_request( sc_input, s_input, s_out);						
						break;
				case 2:						
						while(!((get1=s_input.readLine()).equals("Exit")))
						{						
					//	s_out.println("done");
						//s_out.println(list.get(get1));
						xxxx=list.get(get1);
					//	s_out.flush();
						}
						//incoming_socket.close();
						Server_request( sc_input, s_input, s_out);
						break;
				case 3:
						while(!((delete1=s_input.readLine()).equals("Exit")))
						{
							//delete1=s_input.readLine();							
							//s_out.println(list.remove(delete1));
							xxxx=list.remove(delete1);
						}
						Server_request( sc_input, s_input, s_out);
						break;					
			}				
		}
		catch(Exception e)
		{
		//	System.out.println("");
		}
	}
	synchronized public void client_side() throws Exception
	{
		try
		{			
			int x1;
			BufferedReader sc_input1=new BufferedReader(new InputStreamReader(System.in));     
			System.out.println("Enter 1 to continue");
			x1=Integer.parseInt(sc_input1.readLine());
			for(int z=thread_start;z<=thread_end;z++)
			{
				System.out.println("Ip is "+ip[z]);
				System.out.println("Port is "+port[z]);
				System.out.println();
			}
			for(int z=thread_start;z<=thread_end;z++)
			{
				System.out.println("z before" +z);
				InetAddress ia1=InetAddress.getByName(ip[z]);
				System.out.println("z after inet" +ia1);				
				//System.out.println("z after sx" +sx);
				s2_array[z] =new Socket(ia1,port[z]);
				System.out.println("z after socket" +z);
				s_input2_array[z]=new BufferedReader(new InputStreamReader(s2_array[z].getInputStream())); 
				System.out.println("z after BR" +z);
				s_out2_array[z]=new PrintWriter(s2_array[z].getOutputStream(),true);
				System.out.println("z after PW" +z);
			}
			System.out.println("Hi I am here ");
			Client_Choice(sc_input1,s2_array,s_input2_array,s_out2_array);								
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
	synchronized public void Client_Choice(BufferedReader sc_input1,Socket[] s2_array,BufferedReader[] s_input2_array,PrintWriter[] s_out2_array)
	{
		try
		{
		
		int choice;
		String repeat_menu;		
		System.out.println("1: Insert");
		System.out.println("2: Lookup");
		System.out.println("3: Remove");
		System.out.println("4: Exit");
		System.out.println();
		System.out.println("Enter your choice:- ");		
		choice=Integer.parseInt(sc_input1.readLine());
		
		switch(choice)
		{
			case 1: generate_key_value();
					client_put(s2_array,s_input2_array,s_out2_array);					
					System.out.println("Do you want to repeat menu (Y/N) ?");
					repeat_menu=sc_input1.readLine();											//	This code will ask user if he wants to repeat oparation or not if
					if((repeat_menu.equals("Y")) || (repeat_menu.equals("y")))											//	Y then call begin() method to repeat menu else 
					{																		//	else closes session.
						//c_out.println(repeat_menu);						
						Client_Choice(sc_input1,s2_array,s_input2_array,s_out2_array);
					}
					else
						System.exit(0);
					break;
			case 2: client_get(s2_array,s_input2_array,s_out2_array);
					System.out.println("Do you want to repeat menu (Y/N) ?");
					repeat_menu=sc_input1.readLine();											//	This code will ask user if he wants to repeat oparation or not if
					if((repeat_menu.equals("Y")) || (repeat_menu.equals("y")))											//	Y then call begin() method to repeat menu else 
					{																		//	else closes session.
						//c_out.println(repeat_menu);						
						Client_Choice(sc_input1,s2_array,s_input2_array,s_out2_array);
					}
					else
						System.exit(0);
					break;
			case 3: client_delete(s2_array,s_input2_array,s_out2_array);
					System.out.println("Do you want to repeat menu (Y/N) ?");
					repeat_menu=sc_input1.readLine();											//	This code will ask user if he wants to repeat oparation or not if
					if((repeat_menu.equals("Y")) || (repeat_menu.equals("y")))											//	Y then call begin() method to repeat menu else 
					{																		//	else closes session.
						//c_out.println(repeat_menu);						
						Client_Choice(sc_input1,s2_array,s_input2_array,s_out2_array);
					}
					else
						System.exit(0);
					break;
			case 4:
						System.exit(0);
						break;	
		   default:
					break;
		}		
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
	void generate_key_value()
	{
		int z=1;
	
		int key_start_c=start_counter;
		String temp="";
		for(int k=1;k<keysize;k++)
		{		
			temp="";
			i_key[k]= "";
			i_val[k]="";
			for(z=1;z<(10-(i_key[k].length()));z++)
			{
				temp=temp+charArray[new Random().nextInt(26)];				
			}
			i_key[k]=temp;
			temp="";	
			for(z=1;z<(90-(i_val[k].length()));z++)
			{
				temp=temp+charArray[new Random().nextInt(26)];				
			}
			i_val[k]=temp;
			key_start_c++;
			//System.out.println("Key "+i_key[k]);
			//System.out.println("Val "+i_val[k]);
		}
		//System.out.println("This is done");
	}
	synchronized public void client_put(Socket[] s2_array1,BufferedReader[] s_input2_array1,PrintWriter[] s_out2_array1)
	{
		int start_c=1;
		try
		{
			String key,value,xxxx;
			char c;
			int add,v,index,port_no;
			int xyz=10000000;			
			//int []port_list={25003,25001,25002};
		//	BufferedReader c_put=new BufferedReader(new InputStreamReader(System.in));     			
			System.out.println("Enter the key:-");
			
		for(int i=thread_start;i<=thread_end;i++)
			{
					s_out2_array1[i].println("1");
			}
			
			
			//System.out.println(" Put Operations done !!!");
			Date date = new Date();
			System.out.println("Put Start time is:- "+new Timestamp(date.getTime()));
			
		for(int j=start_counter;j<=end_counter;j++)
		{  index=0;
			for(int k=0;k<9;k++)
			{
				index=index+i_key[j].charAt(k);
			}
			index=index%instance;
				
			s_out2_array1[index].println(i_key[start_c]);
		//	s_out2_array1[index].flush();
			//xxxx=s_input2_array1[index].readLine();
			s_out2_array1[index].println(i_val[start_c]);
		    start_c++;
			}
			Date date1 = new Date();
		//	System.out.println("Put Start time is:- "+new Timestamp(date.getTime()));			
			System.out.println("Put End time is:-   "+new Timestamp(date1.getTime()));
			//System.out.println("Key from 1000000 to 1999999 put successfully !!!!");
			long diff=date1.getTime()-date.getTime();
			System.out.println("Time taken by put operations is (Miliseconds):- "+diff);
			for(int i=thread_start;i<=thread_end;i++)
			{
					s_out2_array1[i].println("Exit");
			}

			//client_get(s2_array1,s_input2_array1,s_out2_array1);
			
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
	synchronized public void client_get(Socket[] s2_array1,BufferedReader[] s_input2_array1,PrintWriter[] s_out2_array1)
	{
		int start_c=1;
		try
		{
		String get_key,get_string,xxxx;
		char c;
	//	BufferedReader c_put=new BufferedReader(new InputStreamReader(System.in)); 
		int add,v,port_no;
		int index;
		//System.out.println("Enter key to get value:- ");
		//Date date = new Date();
		for(int i=thread_start;i<=thread_end;i++)
			{
					s_out2_array1[i].println("2");
			}
				
		Date date = new Date();
	System.out.println("Start time is:- "+new Timestamp(date.getTime()));
	for(int j=start_counter;j<=end_counter;j++)
	{
			index=0;	
	//		add=0;
		
			for(int k=0;k<9;k++)
			{
				index=index+i_key[j].charAt(k);
			}
			index=index%instance;
					
			s_out2_array[index].println(i_key[start_c]);
			start_c++;
		}
			Date date1 = new Date();
			//System.out.println("Start time is:-  "+new Timestamp(date.getTime()));			
			System.out.println("End time is:-    "+new Timestamp(date1.getTime()));
			long diff=date1.getTime()-date.getTime();
			System.out.println("Time taken by get operations is (Miliseconds):- "+diff);
			//System.out.println("Key from 1000000 to 1999999 get sucessfully  !!!!");
			
			for(int i=thread_start;i<=thread_end;i++)
			{
					s_out2_array1[i].println("Exit");
			}
		   // client_delete(s2_array1,s_input2_array1,s_out2_array1);
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
	synchronized public void client_delete(Socket[] s2_array1,BufferedReader[] s_input2_array1,PrintWriter[] s_out2_array1)
	{
		int start_c=1;
		try
		{		
		String delete_key;
	//	BufferedReader c_put=new BufferedReader(new InputStreamReader(System.in)); 
		int add,v,index,port_no;
		char c;			
		//System.out.println("Enter key to delete value:- ");
	//	delete_key=c_put.readLine();
		add=0;
			
			for(int i=thread_start;i<=thread_end;i++)
			{
					s_out2_array1[i].println("3");
			}
			
				Date date = new Date();
				System.out.println("Start time is:- "+new Timestamp(date.getTime()));
			for(int j=start_counter;j<=end_counter;j++)
			{	
				index=0;
				for(int k=0;k<9;k++)
			{
				index=index+i_key[j].charAt(k);
			}
			index=index%instance;
				//s_out2_array1[index].println("3");
				s_out2_array1[index].println(i_key[start_c]);	
			//	System.out.println(s_input2_array1[index].readLine()+" value deleted from server");
				start_c++;
			}
			Date date1 = new Date();
		//	System.out.println("Start time is:-  "+new Timestamp(date.getTime()));			
			System.out.println("End time is:-    "+new Timestamp(date1.getTime()));
			long diff=date1.getTime()-date.getTime();
			System.out.println("Time taken by delete operations is (Miliseconds):- "+diff);
			//System.out.println("Key from 1000000 to 1999999 deleted sucessfully !!!!");
			
			for(int i=thread_start;i<=thread_end;i++)
			{
					s_out2_array1[i].println("Exit");
			}		
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
	 public static void main(String args[]) throws Exception
	{
		try
		{			
			int i,j;
			String rline;
			File server_address=new File("Servers.txt");
			if(!server_address.exists())
			{
				System.out.println("File does not exist !!!");
			}
			else
			{
				FileReader fr=new FileReader("Servers.txt");
				BufferedReader br=new BufferedReader(fr);
				i=0;
				j=instance;
				while((rline = br.readLine()) != null)  
					{
						System.out.println(rline);
						ip[i]=rline.substring(0,rline.indexOf(" "));	
						replica_ip[j]=rline.substring(0,rline.indexOf(" "));	
						System.out.println("Ip is "+ip[i]);
						port[i]=Integer.parseInt(rline.substring(rline.indexOf(" ")+1));
						replica_port[j]=Integer.parseInt(rline.substring(rline.indexOf(" ")+1));
						System.out.println("Port is "+port[i]);
						i++;	
						j--;
					}    					
					br.close();     
			}
			
			Client_Test_P ct=new Client_Test_P("Client");
			Thread t=new Client_Test_P("Server");		
			t.start();
			ct.client_side();
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
}
