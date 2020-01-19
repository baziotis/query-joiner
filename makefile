CC = g++
CFLAGS = -Wall -ggdb -O3

bin: command_interpreter.o file_manager.o intermediate_result.o joinable.o main.o parse.o query_executor.o relation_data.o relation_storage.o report_utils.o task_scheduler.o tokenizer.o utils.o 
	$(CC) $(CFLAGS) command_interpreter.o file_manager.o intermediate_result.o joinable.o main.o parse.o query_executor.o relation_data.o relation_storage.o report_utils.o task_scheduler.o tokenizer.o utils.o -o query_joiner -lm -lpthread 

command_interpreter.o : command_interpreter.cpp command_interpreter.h utils.h 
	$(CC) $(CFLAGS) -c command_interpreter.cpp 

file_manager.o : file_manager.cpp file_manager.h 
	$(CC) $(CFLAGS) -c file_manager.cpp 

intermediate_result.o : intermediate_result.cpp intermediate_result.h 
	$(CC) $(CFLAGS) -c intermediate_result.cpp 

joinable.o : joinable.cpp joinable.h report_utils.h 
	$(CC) $(CFLAGS) -c joinable.cpp 

main.o : main.cpp command_interpreter.h parse.h relation_storage.h query_executor.h 
	$(CC) $(CFLAGS) -c main.cpp -lm 

parse.o : parse.cpp parse.h 
	$(CC) $(CFLAGS) -c parse.cpp 

query_executor.o : query_executor.cpp query_executor.h report_utils.h 
	$(CC) $(CFLAGS) -c query_executor.cpp 

relation_data.o : relation_data.cpp relation_data.h joinable.h 
	$(CC) $(CFLAGS) -c relation_data.cpp 

relation_storage.o : relation_storage.cpp relation_storage.h utils.h report_utils.h 
	$(CC) $(CFLAGS) -c relation_storage.cpp 

report_utils.o : report_utils.cpp report_utils.h 
	$(CC) $(CFLAGS) -c report_utils.cpp 

task_scheduler.o : task_scheduler.cpp task_scheduler.h report_utils.h 
	$(CC) $(CFLAGS) -c task_scheduler.cpp -lpthread 

tokenizer.o : tokenizer.cpp tokenizer.h 
	$(CC) $(CFLAGS) -c tokenizer.cpp 

utils.o : utils.cpp utils.h common.h 
	$(CC) $(CFLAGS) -c utils.cpp 

.PHONY : clear

clear :
	rm -f query_joiner command_interpreter.o file_manager.o intermediate_result.o joinable.o main.o parse.o query_executor.o relation_data.o relation_storage.o report_utils.o task_scheduler.o tokenizer.o utils.o 


#Generated with makefile generator: https://github.com/GeorgeLS/Makefile-Generator/blob/master/mfbuilder.c
