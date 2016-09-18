package src.Couch_Code;
import java.util.Scanner;
public class Couch_Evaluation {

	public static void main(String[] args) {

		Scanner scanner_obj = new Scanner(System.in);
		String user_choice = null;
		Couch_Client couch_client_obj = new Couch_Client();

		couch_client_obj.createRandomKeys();
		
		couch_client_obj.initializeLogger();	

		do {			
			System.out.println("Enter Your Choice :- ");
		
		System.out.println("1.Insert Element .\n2.Lookup Element .\n3.Remove Element .\n4.Exit");
		
		user_choice = scanner_obj.nextLine();
			if ("1".equalsIgnoreCase(user_choice) || "2".equalsIgnoreCase(user_choice) || "3".equalsIgnoreCase(user_choice)) {
				switch (user_choice) {

		case "1":
					couch_client_obj.put();
					break;
		
		case "2":
					couch_client_obj.get();
					break;
		
		case "3":
					couch_client_obj.delete();
					break;
				}
			} else if (!"4".equals(user_choice)) {
				System.out.println("Please Enter Correct Choice.");
			}
		} while (!"4".equals(user_choice));

	}
}