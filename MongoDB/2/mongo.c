#include <bson.h>
#include <mongoc.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h> 

int total_operations=100000;
void gen_key_val(char *key_val_array[total_operations][2]);
void execute_operations(char *key_val_array[total_operations][2]);
int main (int   argc,char *argv[])
{
	 mongoc_init ();
     int itr=0,i;
	 int array_size=total_operations;
	 int current_instance=2;
	 const char *ip_address[16]= {"172.31.15.46", "172.31.15.168"}; 
     char *port[16]={"27017","27017"};
     mongoc_client_t *client1[16];
     mongoc_collection_t *collection1[16];
     bson_t *query;
     mongoc_cursor_t *cursor;
     bson_error_t error;
     bson_oid_t oid;
	 char *c_address[16];
	 int charchoice=1;
     int choice;
	 double diff=0;
	 int hashvalue=0;
	
	struct timespec start, stop;  
	 bson_t *doc;
	 double mili;
	 int key_find;
	   
	 struct timeval timeout = { 1, 500000 }; // 1.5 seconds    
	 char *key_val_array[total_operations][2];

	

    gen_key_val(key_val_array);  

  for (i=0;i<current_instance;i++)
   {
		char *connect_add=(char*)sizeof(sizeof(char)*100);
        char ch[100]="mongodb://";
		connect_add=strcat(strcat(ch,ip_address[i]),":27017");
        client1[i] = mongoc_client_new (connect_add);
        collection1[i] = mongoc_client_get_collection (client1[i], "mydb", "mycoll");
    }    	 
	 do
	{	
	printf("\n");
    printf("\n\tMongoDb Database\nPerform %d operation  \n1.Insert\n2.Lookup\n3.Remove\nEnter your choice: ",total_operations);
    scanf("%d",&choice);

	  if(choice==1)
	   {
		if( clock_gettime( CLOCK_REALTIME, &start) == -1 ) { 
      perror( "clock gettime" ); 
      return EXIT_FAILURE; 
    }
	   for (i=0;i<total_operations;i++)
        {
				for(key_find=0;*(key_val_array[i][0]+key_find)!='\0';key_find++)
					hashvalue=hashvalue+*(key_val_array[i][0]+key_find);
				hashvalue=i%current_instance;
                doc = bson_new ();
                bson_append_utf8 (doc,"key" ,3,key_val_array[i][0], 10);
                bson_append_utf8 (doc,"value",5,key_val_array[i][1], 90);                
               
				if(hashvalue==0)
                {
                   if (!mongoc_collection_insert (collection1[hashvalue], MONGOC_INSERT_NONE, doc, NULL, &error))
                   {
                        fprintf (stderr, "%s\n", error.message);
                   }
                }				
              
				bson_destroy (doc);				
				
        }
		}
		else if(choice==2)
		{
		
			if( clock_gettime( CLOCK_REALTIME, &start) == -1 ) { 
      perror( "clock gettime" ); 
      return EXIT_FAILURE; 
    }
			for (i=1;i<array_size;i++)
			{
				for(key_find=0;*(key_val_array[i][0]+key_find)!='\0';key_find++)
					hashvalue=hashvalue+*(key_val_array[i][0]+key_find);
				hashvalue=i%current_instance;
				query = bson_new ();
				bson_append_utf8 (query ,"key" ,3,key_val_array[i][0], 10);
				bson_append_utf8 (query ,"value",5,key_val_array[i][1], 90);
				
				
				if(!mongoc_collection_find (collection1[hashvalue], MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL))
				{
					printf("Key does not exist");
				}
				
				bson_destroy (query);
				
			}	
		}
		else if(choice==3)
		{
		
			if( clock_gettime( CLOCK_REALTIME, &start) == -1 ) { 
      perror( "clock gettime" ); 
      return EXIT_FAILURE; 
    }
			for (i=1;i<array_size;i++)
			{
				for(key_find=0;*(key_val_array[i][0]+key_find)!='\0';key_find++)
					hashvalue=hashvalue+*(key_val_array[i][0]+key_find);
				hashvalue=i%current_instance;
				doc = bson_new ();
				BSON_APPEND_OID (doc, "_id", &oid);	  
				
					if (!mongoc_collection_remove (collection1[hashvalue], MONGOC_REMOVE_SINGLE_REMOVE, doc, NULL, &error)) 
					{
						printf ("Delete failed: %s\n", error.message);
					}
				
				bson_destroy (doc);
				
			}
		}

	if( clock_gettime( CLOCK_REALTIME, &stop) == -1 ) { 
      perror( "clock gettime" ); 
      return EXIT_FAILURE; 
	  }
		// Stop clock
	 long seconds = stop.tv_sec - start.tv_sec;
     long mili_seconds = stop.tv_nsec - start.tv_nsec;
     if (mili_seconds < 0){
        seconds -= 1;
      }
      long total_mili_seconds = (seconds * 1000000000) + abs(mili_seconds);
      float total_seconds=  total_mili_seconds/1000000000;
      printf( "Total time required:  \n1.   %ld (milliSecond)\n",total_mili_seconds/1000000); 
	  
	printf("\n Enter 1 to repeat menu:- ");
	scanf(" %d",&charchoice);	

	}while(charchoice==1);		
	
	for(i=0;i<current_instance;i++)
	{
		mongoc_collection_destroy (collection1[i]);
		mongoc_client_destroy (client1[i]);
	}
    mongoc_cleanup ();
	
	//execute_operations(key_val_array);   
    
    return 0;
}
/*void execute_operations(char *key_val_array[total_operations][2])
{
	 
  
}*/
void gen_key_val(char *key_val_array[total_operations][2]){
       
        int iK,iV,i;
 
        char alpha_array[]={'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};
         for(i=0;i<total_operations;i++){
                 //Random key generation
                key_val_array[i][0]=(char*)malloc(sizeof(char)*10);
                
				key_val_array[i][1]=(char*)malloc(sizeof(char)*90);// randomKey

                 for(iK=0;iK<10;iK++){
                        *(key_val_array[i][0]+iK)=alpha_array[(rand() % 26)];
                 }
                *(key_val_array[i][0]+iK)='\0';

                 //Random value generation
                 for(iV=0;iV<90;iV++){
                        *(key_val_array[i][1]+iV)=alpha_array[(rand() % 26)];
                 }
                *(key_val_array[i][1]+iV)='\0';            
         }
}