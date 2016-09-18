#include <stdio.h> 
#include <stdlib.h> 
#include <string.h> 
#include <hiredis/hiredis.h>
#include <unistd.h>
#include <time.h>

int total_operations=100000; 
void gen_key_val(char *key_val_array[total_operations][2]); 
int execute_operations(int instance,char *key_val_array[total_operations][2],redisContext *connection[16],redisReply *reply[16]);
int main() 
{
    int i;
    int instance=1;
    const char *ip_address[16] = {"172.31.15.46"}; 
    int port[16]={6379};

    char *key_val_array[total_operations][2];     
    struct timeval timeout = { 1, 500000 }; // 1.5 seconds    
    redisContext *connection[16];
     
    for(i=0;i<instance;i++){
    connection[i] = redisConnectWithTimeout(ip_address[i], port[i], timeout);   
    }

    for(i=0;i<instance;i++){
        if (connection[i] == NULL || connection[i]->err) { 
            if (connection[i]) { 
            printf("Connection error: %s\n", connection[i]->errstr); 
            redisFree(connection[i]); 
        } else { 
          printf("Connection error: can't allocate redis context\n"); 
        } 
        exit(1); 
        }
    }

   redisReply *reply[16];
   for(i=0;i<instance;i++){
     reply[i]= redisCommand(connection[i],"DEL mylist");    
     freeReplyObject(reply[i]);
   }     
	gen_key_val(key_val_array);   /// Call key value generation 
    execute_operations(instance,key_val_array,connection,reply);

     /* Disconnects and frees the context */
     for(i=0;i<instance;i++){ 
         redisFree(connection[i]);
     }
     return 0; 
}
int execute_operations(int instance,char *key_val_array[total_operations][2],redisContext *connection[16],redisReply *reply[16])
{
		int i;
	  int array_size=total_operations; 
	 int instance_no,choice;
	 int charchoice=1;
	 int key_find;
	// struct tm start1, stop1;
     double accum;
	 double diff=0;
	 struct timespec start, stop;
	do
	{	
	
	printf("\n");
    
	printf("\n\tRedis Database\nPerform %d operation  \n1.Insert\n2.Lookup\n3.Remove\nEnter your choice: ",total_operations);
    
	scanf("%d",&choice);
	 
/// Start timing

    if(choice==1) // Set operation
    {	
		printf("\nInsert Operation\n");
		if( clock_gettime( CLOCK_REALTIME, &start) == -1 ) { 
      perror( "clock gettime" ); 
      return EXIT_FAILURE; 
    }
	for (i = 0; i < array_size; i++) {     
       for(key_find=0;*(key_val_array[i][0]+key_find)!='\0';key_find++)
					instance_no=instance_no+*(key_val_array[i][0]+key_find);
				
				
	   instance_no=instance_no%instance;
       if(instance_no<instance){
         reply[instance_no] = redisCommand(connection[instance_no],"SET %s %s",key_val_array[i][0],key_val_array[i][1]); 
             freeReplyObject(reply[instance_no]);
    }
    }
	}
         
    else if(choice==2)//GET Operation
    {
		printf("\nLookup Operation\n");
		if( clock_gettime( CLOCK_REALTIME, &start) == -1 ) { 
      perror( "clock gettime" ); 
      return EXIT_FAILURE; 
	  }
        for (i = 0; i < array_size; i++) {
           instance_no=instance_no+*(key_val_array[i][0]+key_find);
			
				
	   instance_no=instance_no%instance;
           if(instance_no<instance){
             reply[instance_no] = redisCommand(connection[instance_no],"GET %s",key_val_array[i][0]);
             //printf("Value: %s\n",reply[instance_no]->str); 
             freeReplyObject(reply[instance_no]);
        } 
        } 
    }    
   else if(choice==3)//DELETE OPERATION
   {
		printf("\nRemove Operation\n");
		if( clock_gettime( CLOCK_REALTIME, &start) == -1 ) { 
      perror( "clock gettime" ); 
      return EXIT_FAILURE; 
	  }
         for (i = 0; i < array_size; i++){
          instance_no=instance_no+*(key_val_array[i][0]+key_find);
				
				
	   instance_no=instance_no%instance;
           if(instance_no<instance){
             reply[instance_no] = redisCommand(connection[instance_no],"DEL %s",key_val_array[i][0]);
             freeReplyObject(reply[instance_no]);
        } 
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
      printf( "Total time required:  \n1.   %ld (milli_seconds)\n",total_mili_seconds/1000000); 

	printf("\n Enter 1 to repeat menu:- ");
	scanf(" %d",&charchoice);	
    
	}while(charchoice==1);
}
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