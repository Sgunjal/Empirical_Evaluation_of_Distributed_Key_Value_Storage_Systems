package sgunjal.couch;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

final public class ReadConfFile
{
	
	public Properties loadConfig()
	{
		final Properties conf=new Properties();
		FileInputStream readFile;
		try
		{
			readFile = new FileInputStream("conf/conf.xml");
		
		conf.loadFromXML(readFile);
		readFile.close();
		return conf;
		} catch (FileNotFoundException e)
		{
			System.out.println("File not found");
			e.printStackTrace();
		}
		catch(InvalidPropertiesFormatException e)
		{
			System.out.println("Invalid Properties file");
		}
		catch(IOException e)
		{
			System.out.println("Cannot close conf file");
		}
		return null;
	}
	public Properties loadConfig(String path)
	{
		final Properties conf=new Properties();
		FileInputStream readFile;
		try
		{
			readFile = new FileInputStream(path);
		
		conf.loadFromXML(readFile);
		readFile.close();
		return conf;
		} catch (FileNotFoundException e)
		{
			System.out.println("File not found");
			e.printStackTrace();
		}
		catch(InvalidPropertiesFormatException e)
		{
			System.out.println("Invalid Properties file");
		}
		catch(IOException e)
		{
			System.out.println("Cannot close conf file");
		}
		return null;
	}

	
}
