#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#define MAXFIELDS 100 // for now
#define MAXINPUTLENGTH 5000
#define MAXLENOFFIELDNAMES 50
#define MAXLENOFFIELDTYPES 50

struct _field {
   char fieldName[MAXLENOFFIELDNAMES];
   char fieldType[MAXLENOFFIELDTYPES];
   int fieldLength;
}; 
struct _table {
   char *theName;
   char *tableFileName;
   char *schemaFileName;
   int reclen;
   int fieldcount;
   struct _field fields[MAXFIELDS]; 
   struct _table* next; // added next pointer
};
typedef enum{false, true} bool;
// where struct
struct where { // added where struct
      char* left;
      char* right;
      bool isConstant;
      bool isLessThan;
      bool isGreaterThan;
      struct where* next;
};
struct fieldSelect { // added field struct
      char* name;
      bool isWhere;
      struct fieldSelect* next;
};

bool NOW = false;

void trimwhitespace(char *trimStr);
void processCommand(char *theBuffer, FILE *inputFile, int showCommands);
struct _table* addTablesList(char *fromClause);

bool loadDatabase(struct _table *table, char *theBuffer) { // copied from assignment 2
   //printf("NUM FIELDS: %d\n", table->fieldcount);
   //printf("*** LOG: Loading database from input ***\n");
   FILE *fp;
   char dbFile[500]; // store database file name
   strcpy(dbFile, table->tableFileName);
   //printf("THE TABLE TO LOAD TO: %s\n", dbFile);
   fp = fopen(dbFile, "ab+"); // open binary file for write
   if (fp == NULL) 
   {
      printf("Error opening file");
   }
   const char delim[2] = ",";
   char strInput[MAXINPUTLENGTH]; 
   char *nextRecord;
   char *token;
   int numFields;
   int offset;
   //printf("NUM FIELDS: %d\n", table->fieldcount);
   numFields = table->fieldcount; // number of fields for each record
   bool keepGoing = true;
   //while (keepGoing == true) 
   //{
   strcpy(strInput, theBuffer);
      //fgets(strInput, sizeof(strInput)-1, stdin);
   nextRecord = calloc(1, table->reclen); // initialize pointer to store records
   offset = 0;
   if (strcmp(strInput, "\n") == 0 || strcmp(strInput, "END\n") == 0) {
      //printf("*** LOG: Closing file\n"); 
      printf("===> END\n");
      keepGoing = false;
   }
   else 
   {
      //printf("*** LOG: Loading input data starting with [%20.20s]\n", strInput);
      trimwhitespace(strInput);
      //printf("===> %s\n", strInput);
      //fflush(stdout);
      int i = 0;
      while (i < numFields) {
         if (i == 0) {
            token = strtok(strInput, delim);                
         }
         else {
            token = strtok(NULL, delim);   
         }
         if (strlen(token) > table->fields[i].fieldLength) {
            printf("*** WARNING: Data in field %s is being truncated ***\n", table->fields[i].fieldName); // modify this    
         }

         if (i + 1 == numFields) {
            token[strcspn(token, "\n")] = 0;
         }
         //printf("TABLE FIELD LENGTH: %d\n", table->fields[i].fieldLength);
         strncpy(&nextRecord[offset], token, table->fields[i].fieldLength-1); // copy into buffer
         offset = offset + table->fields[i].fieldLength; // increment offset      
         i = i + 1;
      }  
   } // end of else statement
   fwrite(nextRecord, table->reclen, 1, fp); // writing memory to binary file
   free(nextRecord); // free memory
   //} // end of while loop
   fclose(fp); // close file
}

// GET THE RECORD FROM THE FILE BY FSEEKING TO THE RIGHT SPOT AND READING IT
bool getRecord(int recnum, char *record, struct _table *table) {
   //printf("*** LOG: Getting record %d from the database ***\n", recnum);
   FILE *fp;
   char dbFile[500]; // store database file name
   strcpy(dbFile, table->tableFileName);
   fp = fopen(dbFile, "rb"); // open binary file for read
   if (fp == NULL) 
   {
      printf("Error opening file");
   }
   fseek(fp, recnum * (table->reclen), SEEK_SET);
   fread(record, table->reclen, 1, fp);
   fclose(fp);
   return true;
}

void trimwhitespace(char *trimStr) { 
   char *end;
   while (isspace((*trimStr))) { // get starting white spaces
      trimStr++;
   }
   if(*trimStr != 0) {
      end = trimStr + strlen(trimStr) - 1;
      while(isspace(*end) && end > trimStr) { // get ending
         end--;
      }
      *(end+1) = 0; // add null terminator
   }
}

void getTableName(char *name, char *loadBuffer, char *remainingBuffer) {
   //char entireBuffer[MAXINPUTLENGTH];
   //strcpy(entireBuffer, loadBuffer); // copying entire original buffer into new one
   char* token;
   token = strtok(loadBuffer, " "); // INSERT
   token = strtok(NULL, " "); // INTO
   token = strtok(NULL, " "); // table name
   //token[strcspn(token, "\n")] = 0; // remove newline character at end of string
   strcpy(name, token);
   token = strtok(NULL, ""); // remaining buffer
   strcpy(remainingBuffer, token);
}

void getFields(struct _table *addTable, int toPrint) {
   char fieldName[5000]; //strcpy(fieldName, "$$$$$$$$$$$$$$$$$$$$");
   bool stillAdd = true;
   int fieldCounter = 0;
   int lengthCounter = 0;
   FILE *fp;
   //printf("TABLE NAME: [%s]\n", addTable->schemaFileName); fflush(stdout);
   if (NOW)
         fp = fopen("shaffer.dat", "r"); // read binary ?? correct mode??
   else
      fp = fopen(addTable->schemaFileName, "r"); // read binary ?? correct mode??
   if (fp == NULL) {
      printf("ACK!\n");
      exit(0);
   }
   //fflush(stdout);
   //fseek(fp, 0, SEEK_SET);
   while (stillAdd) {
      //printf("STILL ADDING:\n");fflush(stdout);
      fgets(fieldName, sizeof(fieldName)-1, fp); 
      //printf("INPUT BUFFER: [%s]\n", fieldName); fflush(stdout);
      if (strcmp(fieldName, "END\n")==0) {
         //printf("DONE!\n"); fflush(stdout);
         stillAdd = false;
      } else {
         char* token;
         char fieldN[100];
         char fieldT[100];
         int fieldL;
         token = strtok(fieldName, " ");
          //printf("INPUT BUFFER [2]: [%s]\n", fieldName); fflush(stdout);
         strcpy(fieldN, strtok(NULL, " ")); // the field name
         strcpy(fieldT, strtok(NULL, " ")); // the field type
         fieldL = atoi(strtok(NULL, " ")); // ?? check // the field length ??
         strcpy(addTable->fields[fieldCounter].fieldName,fieldN); // adding field name
         strcpy(addTable->fields[fieldCounter].fieldType,fieldT); // adding field type
         addTable->fields[fieldCounter].fieldLength = fieldL; // adding field length
         //printf("*** LOG: ADDING FIELD [%s] [%s] [%d]\n",fieldN, fieldT, fieldL);
         if (toPrint == 1) {
            //printf("--- %s (%s-%d)\n", fieldN, fieldT, fieldL);
         }
         lengthCounter = lengthCounter + fieldL;
         fieldCounter++; // increment number of fields added
          //printf("END OF ELSE\n");fflush(stdout);
      } // END OF ELSE     
   //printf("END OF WHILE\n");fflush(stdout);
   } // END OF WHILE LOOP
   //printf("OUT OF WHILE\n");fflush(stdout);
   addTable->reclen = lengthCounter;
   addTable->fieldcount = fieldCounter;
   //printf("NUM FIELDS: %d\n", addTable->fieldcount);
   //printf("ADD TABLE RECLEN: %d\n", addTable->fieldcount);
   //printf("ABOUT TO CLOSE FILE\n");fflush(stdout);
   fclose(fp); // close file
}

// READ THE DATA FROM STDIN AS THE DESIGN OF THE DATABASE TABLE
// LOAD "table" WITH THE APPROPRIATE INFO
bool loadSchema(struct _table *table, char *commandTableName, int showPrints) {
   //printf("*** LOG: Loading table fields...\n");
   if (showPrints == 1) {
      //printf("----------- SCHEMA --------------\n");
   }
   char tableName[MAXINPUTLENGTH];
   char tableFileN[MAXINPUTLENGTH];
   char schemaFileN[MAXINPUTLENGTH];
   strncpy(tableName, commandTableName, strlen(commandTableName)+1);
   //printf("THE COPIED TABLE NAME !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! %s\n", tableName);
   if (showPrints == 1) {
      //printf("TABLE NAME: %s\n", tableName);
   }
   table->tableFileName = calloc(1, MAXINPUTLENGTH);
   table->schemaFileName = calloc(1, MAXINPUTLENGTH);
   strncpy(tableFileN, tableName, sizeof(tableName));
   strncpy(schemaFileN, tableName, sizeof(tableName));
   strcat(tableFileN, ".bin\0");
   strcat(schemaFileN, ".schema\0");
   strncpy(table->tableFileName, tableFileN, sizeof(tableFileN));
   strncpy(table->schemaFileName, schemaFileN, sizeof(schemaFileN));
   //printf("TABLE FILE NAME IN LOAD SCHEMA: %s\n", table->tableFileName);
   //fflush(stdout);
   getFields(table, showPrints); // get fields
   return true;
}

int createTable(char *tableName, FILE *input, int show) {
   //printf("*** LOG: Creating table...\n");
   //printf("-----------------------------------------------%s\n", tableName);
   FILE *fp;
   char schemaFile[500]; // store database file name
   char dataFile[500];
   strcpy(dataFile, tableName);
   strcat(dataFile, ".bin\0");
   strcpy(schemaFile, tableName);
   strcat(schemaFile, ".schema\0");
   fp = fopen(dataFile, "wb+"); // open binary file for write
   if (fp == NULL) 
   {
      printf("Error opening .bin file\n");
   }
   fclose(fp);
   fp = fopen(schemaFile, "w+"); // open schema file for write //
   if (fp == NULL) 
   {
      printf("Error opening .schema file\n");
   }
   // read input until END and store in schema file word for word
   char fieldName[100];
   bool stillAdd = true;
   //printf("*** LOG: Loading table fields...\n");
   //printf("*** LOG: Table data name is [%s]\n", dataFile);
   //printf("*** LOG: Table schema name is [%s]\n", schemaFile);
   while (stillAdd) {
      fgets(fieldName, sizeof(fieldName)-1, input); 
      if (strcmp(fieldName,"END\n")==0) {
         fputs(fieldName, fp);
         //printf("*** LOG: END OF CREATE TABLE\n");
         if (show == 1) {
            printf("===> END\n");
         }
         stillAdd = false;
      } else {
         //printf("THE FIELD NAME TO ADD %s\n", fieldName);
         if (show == 1) {
            printf("===> %s", fieldName);
         }
         fputs(fieldName, fp);
         //printf("THE LINE BEING ADDED IN CREATE TABLE: %s\n", fieldName);
      }
   }
   fclose(fp);
}

void showRecord(struct _field *fields, char *record, int fieldCount, struct fieldSelect *head) {
   struct fieldSelect *temp = head;
   int showFieldCounter = 0;
   while (temp != NULL) { // count number of fields to show
      if (temp->isWhere == false) {
         showFieldCounter++;
         //printf("TEMP NAME IS: %s\n", temp->name);
      }
      temp = temp->next;
   }
   char *fieldPrint;
   fieldPrint = calloc(1, MAXINPUTLENGTH);
   temp = head; // set back to beginning of list
   int seenCounter = 0;
   while (temp != NULL) { // loop through fields linked list
      //printf("IN THE LOOP FOR DEPARTMENTS BIN?\n");
      if (temp->isWhere == false) {
         int eachOffset = 0;
         seenCounter++;
         //printf("THE FIELD COUNT IS: %d\n", fieldCount);
         for (int i = 0; i < fieldCount; i++) { // looping through fields array
            //printf("THE FIELD LOOKING FOR IS: %s\n", fields[i].fieldName);
            if (strcmp(fields[i].fieldName, temp->name)==0) { // if the name matches
               //printf("FOUND A MATCH TO CONCAT\n");
               strncat(fieldPrint, &record[eachOffset], fields[i].fieldLength);
               if (fieldPrint[strlen(fieldPrint)-1] != ' ' && (seenCounter != showFieldCounter)) {
                  strcat(fieldPrint, ",");
               }
            }
            eachOffset += fields[i].fieldLength;
         }
      }
      temp = temp->next;
   }
   printf("%s\n", fieldPrint);
   free(fieldPrint);
}

int dropTable(char *theTable) {
   char fileS[MAXINPUTLENGTH]; // store database file name
   char fileB[MAXINPUTLENGTH];
   strcpy(fileB, theTable);
   strcat(fileB, ".bin\0");
   strcpy(fileS, theTable);
   strcat(fileS, ".schema\0");
   //printf("FILE TO REMOVE: %s\n", fileB);
   //printf("OTHER FILE: %s\n", fileS);
   int ret;
   ret = remove(fileS);
   int oret;
   oret = remove(fileB);
   if(ret == 0) {
      //printf("File deleted successfully\n");
   } else {
      printf("Error: unable to delete the file\n");
   }
   return 0;
}

int doesFileExist(const char *filename) {
   struct stat st;
   int result = stat(filename, &st);
   return result;
}

struct _table* addTablesList(char *fromClause) {
   struct _table *head = calloc(1, sizeof(struct _table));
   char* tokenTable;
   const char delim[2] = " ";
   tokenTable = strtok(fromClause, delim); // FROM
   struct _table *temp = head;
   tokenTable = strtok(NULL, delim);
   bool isFirst = true;
   //temp->next = null;
   while (tokenTable != NULL) {
      
      char theTableName[MAXINPUTLENGTH];
      if (tokenTable[strlen(tokenTable)-1] == ',') {
         tokenTable[strlen(tokenTable)-1] = 0;
      }
      trimwhitespace(tokenTable);
      //printf("THE TOKEN: %s\n", tokenTable);
      strncpy(theTableName, tokenTable, strlen(tokenTable)+1);
      //printf("THE TABLE NAME @@@@@@@@@@@@@@@@@@@@@@@@: %s\n", theTableName);
      char schemaFile[500]; // store database file name
      strcpy(schemaFile, theTableName);
      strcat(schemaFile, ".schema\0");
      bool fileHere = true;
      if (doesFileExist(schemaFile) != 0) {
         //printf("Table %s does not exist.\n", selectTable);
         fileHere = false;
      }
      if (fileHere == false) {
         printf("Table %s does not exist.\n", theTableName);
      } else { // file exists
         if (isFirst) {
            //printf("IS FIRST TIMES @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
            //loadSchema(head, theTableName, 0);
            head->theName = calloc(1, MAXINPUTLENGTH);
            strncpy(head->theName, theTableName, sizeof(theTableName));
            head->next = NULL;
            isFirst = false;
         } else {
            //printf("LOADING SECOND TABLE!!!!!!!!!!!!!!!!!!!!\n");
            struct _table *tableForSelect = calloc(1, sizeof(struct _table));
            //printf("SELECT TABLE HERE: %s\n", selectTable);
            //loadSchema(tableForSelect, theTableName, 0); // calling load schema to populate the struct
            tableForSelect->theName = calloc(1, MAXINPUTLENGTH);
            strncpy(tableForSelect->theName, theTableName, sizeof(theTableName));
            temp->next = tableForSelect;
            temp = temp->next;
         }
      }
      tokenTable = strtok(NULL, delim); // table name
      //printf("TOKEN IS: %s\n", tokenTable);
   } // end of token while loop        
   return head;
}

struct where* createWhereList(char *whereClause) {
   struct where *head = calloc(1, sizeof(struct where));
   char* tokenWhere;
   const char delim[2] = " ";
   tokenWhere = strtok(whereClause, delim); // WHERE
   tokenWhere = strtok(NULL, delim); // left field
   head->left = calloc(1, MAXINPUTLENGTH); // allocating memory
   head->right = calloc(1, MAXINPUTLENGTH);
   strncpy(head->left, tokenWhere, strlen(tokenWhere)+1); // copying left field
   tokenWhere = strtok(NULL, delim); // = sign
   tokenWhere = strtok(NULL, delim); // right field
   char* cut = tokenWhere;
   trimwhitespace(cut);
   bool isC = false;
   if (cut[0] == '"') {
      isC = true;
      cut++;
      cut[strlen(cut)-1] = 0;
   }
   strncpy(head->right, cut, strlen(cut)+1); // copying right field
   head->isConstant = isC; // setting boolean
   head->next = NULL;
   return head;
}

struct where* addWhereList(struct where* head, char *andClause) {
   struct where* tempw = head;
   while (tempw->next != NULL) { // iterate to end of linked list
      tempw = tempw->next;
   }
   struct where *newWhere = calloc(1, sizeof(struct where));
   char* tokenWhere;
   const char delim[2] = " ";
   tokenWhere = strtok(andClause, delim); // AND
   tokenWhere = strtok(NULL, delim); // left field
   newWhere->left = calloc(1, MAXINPUTLENGTH); // allocating memory
   newWhere->right = calloc(1, MAXINPUTLENGTH);
   strncpy(newWhere->left, tokenWhere, strlen(tokenWhere)+1); // copying left field
   tokenWhere = strtok(NULL, delim); // = sign OR < OR >
   char* sign = tokenWhere;
   if (strcmp(sign, "<")==0) {
      newWhere->isLessThan = true;
      newWhere->isGreaterThan = false;
   } else if (strcmp(sign, ">")==0) {
      newWhere->isGreaterThan = true;
      newWhere->isLessThan = false;
   } else {
      newWhere->isGreaterThan = false;
      newWhere->isLessThan = false;
   }
   tokenWhere = strtok(NULL, delim); // right field
   char* cut = tokenWhere;
   trimwhitespace(cut);
   bool isC = false;
   if (cut[0] == '"') {
      isC = true;
      cut++;
      cut[strlen(cut)-1] = 0;
   }
   strncpy(newWhere->right, cut, strlen(cut)+1); // copying right field
   newWhere->isConstant = isC; // setting boolean
   newWhere->next = NULL;
   tempw->next = newWhere;
   return head;
}

struct fieldSelect* createIndexFields(char *indexClause) {
   struct fieldSelect *head = calloc(1, sizeof(struct fieldSelect));
   char* tokenField;
   const char delim[2] = " ";
   const char commaDelim[2] = ",";
   tokenField = strtok(indexClause, delim); // CREATE
   tokenField = strtok(NULL, delim); // INDEX
   tokenField = strtok(NULL, delim); // INDEX NAME
   tokenField = strtok(NULL, delim); // USING
   struct fieldSelect *temp = head;
   tokenField = strtok(NULL, commaDelim); // first field name
   bool isFirst = true;
   //temp->next = null;
   while (tokenField != NULL) {
      //printf("THE TOKEN: %s\n", tokenField);
      char theFieldName[MAXINPUTLENGTH];
      while(isspace((unsigned char)(*tokenField))) tokenField++; // remove leading spaces
      if (tokenField[strlen(tokenField)-1] == ',') { // remove any commas
         tokenField[strlen(tokenField)-1] = 0;
      }
      trimwhitespace(tokenField);
      strncpy(theFieldName, tokenField, strlen(tokenField)+1);
      if (isFirst) {
         //printf("IS FIRST TIMES @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
         //loadSchema(head, theTableName, 0);
         head->name = calloc(1, MAXINPUTLENGTH);
         strncpy(head->name, theFieldName, sizeof(theFieldName));
         head->next = NULL;
         head->isWhere = false;
         isFirst = false;
      } else {
         //printf("LOADING SECOND TABLE!!!!!!!!!!!!!!!!!!!!\n");
         struct fieldSelect *fieldForSelect = calloc(1, sizeof(struct fieldSelect));
         //printf("SELECT TABLE HERE: %s\n", selectTable);
         //loadSchema(tableForSelect, theTableName, 0); // calling load schema to populate the struct
         fieldForSelect->name = calloc(1, MAXINPUTLENGTH);
         strncpy(fieldForSelect->name, theFieldName, sizeof(theFieldName));
         fieldForSelect->isWhere = false;
         temp->next = fieldForSelect;
         temp = temp->next;
      }
      tokenField = strtok(NULL, commaDelim); // next field
      //trimwhitespace(tokenField);
      //printf("TOKEN IS: %s\n", tokenField);
   } // end of token while loop   
   return head;
}

struct fieldSelect* createFieldsList(char *selectClause) {
   struct fieldSelect *head = calloc(1, sizeof(struct fieldSelect));
   char* tokenField;
   const char delim[2] = " ";
   const char commaDelim[2] = ",";
   tokenField = strtok(selectClause, delim); // SELECT
   struct fieldSelect *temp = head;
   tokenField = strtok(NULL, commaDelim); // first field name
   bool isFirst = true;
   //temp->next = null;
   while (tokenField != NULL) {
      //printf("THE TOKEN: %s\n", tokenField);
      char theFieldName[MAXINPUTLENGTH];
      while(isspace((unsigned char)(*tokenField))) tokenField++; // remove leading spaces
      if (tokenField[strlen(tokenField)-1] == ',') {
         tokenField[strlen(tokenField)-1] = 0;
      }
      trimwhitespace(tokenField);
      strncpy(theFieldName, tokenField, strlen(tokenField)+1);
      if (isFirst) {
         //printf("IS FIRST TIMES @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
         //loadSchema(head, theTableName, 0);
         head->name = calloc(1, MAXINPUTLENGTH);
         strncpy(head->name, theFieldName, sizeof(theFieldName));
         head->next = NULL;
         head->isWhere = false;
         isFirst = false;
      } else {
         //printf("LOADING SECOND TABLE!!!!!!!!!!!!!!!!!!!!\n");
         struct fieldSelect *fieldForSelect = calloc(1, sizeof(struct fieldSelect));
         //printf("SELECT TABLE HERE: %s\n", selectTable);
         //loadSchema(tableForSelect, theTableName, 0); // calling load schema to populate the struct
         fieldForSelect->name = calloc(1, MAXINPUTLENGTH);
         strncpy(fieldForSelect->name, theFieldName, sizeof(theFieldName));
         fieldForSelect->isWhere = false;
         temp->next = fieldForSelect;
         temp = temp->next;
      }
      tokenField = strtok(NULL, commaDelim); // table name
      //trimwhitespace(tokenField);
      //printf("TOKEN IS: %s\n", tokenField);
   } // end of token while loop   
   return head;
}

struct fieldSelect* addFieldFromWhere(struct fieldSelect* head, char *whereClause) {
   struct fieldSelect* tempf = head;
   while (tempf->next != NULL) { // iterate to end of linked list
      tempf = tempf->next;
   }
   struct fieldSelect *newField = calloc(1, sizeof(struct fieldSelect));
   char* tokenWhere;
   const char delim[2] = " ";
   tokenWhere = strtok(whereClause, delim); // WHERE or AND
   tokenWhere = strtok(NULL, delim); // left field
   newField->name = calloc(1, MAXINPUTLENGTH); // allocating memory
   strncpy(newField->name, tokenWhere, strlen(tokenWhere)+1); // copying left field
   newField->isWhere = true;
   
   struct fieldSelect *newOtherField = calloc(1, sizeof(struct fieldSelect));
   newOtherField->name = calloc(1, MAXINPUTLENGTH);
   tokenWhere = strtok(NULL, delim); // = sign
   tokenWhere = strtok(NULL, delim); // right field
   char* cut = tokenWhere;
   trimwhitespace(cut);
   bool isC = false;
   if (cut[0] == '"') {
      isC = true;
      cut++;
      cut[strlen(cut)-1] = 0;
   }
   strncpy(newOtherField->name, cut, strlen(cut)+1); // copying right field
   newOtherField->isWhere = true;
   
   if (isC) {
      newField->next = NULL;
      free(newOtherField);
   } else {
      newField->next = newOtherField;
        newOtherField->next = NULL;
   }
   tempf->next = newField;
   
   return head;
}

bool checkWhereMatch(char* record, struct _field *fields, int fieldCount, struct where *theWhere) {
   char checkRecordField[MAXINPUTLENGTH];
   int theOffset = 0;
   for (int i = 0; i < fieldCount; i++) {
      if (strcmp(fields[i].fieldName, theWhere->left)==0) {
         strncpy(checkRecordField, &record[theOffset], fields[i].fieldLength);
      }
      theOffset += fields[i].fieldLength;
   }
   if (strcmp(theWhere->right, checkRecordField)==0) {
      return true;
   } else {
      return false;
   }
}

int doBinarySelect(struct fieldSelect *fieldHead, struct _table *tableHead, struct where *whereHead) {
   //printf("REACHED SELECT FUNCTION @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
   FILE *fp;
   size_t nread;
   fp = fopen(tableHead->tableFileName, "rb");
   //printf("SELECT FROM THE TABLE FILE NAME OFFFFFFFFF: %s\n", tableHead->tableFileName);
   fseek(fp, 0, SEEK_END);
   int lengthOfFile = ftell(fp);
   int numRecords = 0;
   if (lengthOfFile == 0 || tableHead->reclen == 0) {
      printf("IS ZERO VALUE FOR LENGTH\n");
   } else {
      numRecords = lengthOfFile / tableHead->reclen;   
   }
   //numRecords--;
   fclose(fp);
   struct where *tempWhere = whereHead;
   char whereCompareField[MAXINPUTLENGTH];
   strcpy(whereCompareField, tempWhere->right); // copy the where field to compare against
   int left = 0;
   int right = numRecords;
   while (left <= right) {
      int mid = (left + right) / 2;
      char *record = calloc(1, tableHead->reclen);
      getRecord(mid, record, tableHead);
      tempWhere = whereHead;
      bool matches = true;
      while (tempWhere != NULL) {
         if (checkWhereMatch(record, tableHead->fields, tableHead->fieldcount, tempWhere) == false) {
            matches = false;
            break;
         }
         tempWhere = tempWhere->next;
      }
      struct fieldSelect *temp = fieldHead;
      int showFieldCounter = 0;
      while (temp != NULL) { // count number of fields to show
         if (temp->isWhere == false) {
            showFieldCounter++;
         }
         temp = temp->next;
      }
      bool isFirstOne = true;
      char saveFirstField[MAXINPUTLENGTH]; // to save the first field to compare
      char *fieldPrint;
      fieldPrint = calloc(1, MAXINPUTLENGTH);
      temp = fieldHead; // set back to beginning of list
      int seenCounter = 0;
      while (temp != NULL) { // loop through fields linked list
         if (temp->isWhere == false) {
            int eachOffset = 0;
            seenCounter++;
            for (int i = 0; i < tableHead->fieldcount; i++) { // looping through fields array
               if (strcmp(tableHead->fields[i].fieldName, temp->name)==0) { // if the name matches
                  if (isFirstOne == true) {
                     strcpy(saveFirstField, &record[eachOffset]); // save the first field
                     isFirstOne = false;
                  }
                  strncat(fieldPrint, &record[eachOffset], tableHead->fields[i].fieldLength);
                  if (fieldPrint[strlen(fieldPrint)-1] != ' ' && (seenCounter != showFieldCounter)) {
                     strcat(fieldPrint, ",");
                  }
               }
               eachOffset += tableHead->fields[i].fieldLength;
            }
         }
         temp = temp->next;
      }
      int compareResult = strcmp(saveFirstField, whereCompareField);
      if (compareResult == 0) {
         //printf("FOUND MATCH!\n");
         printf("TRACE: %s\n", fieldPrint);
         showRecord(tableHead->fields, record, tableHead->fieldcount, fieldHead);
         return 1;
      } else if (compareResult < 0) {
         printf("TRACE: %s\n", fieldPrint);
         left = mid + 1;
      } else {
         printf("TRACE: %s\n", fieldPrint);
         right = mid - 1;
      }
      free(fieldPrint);
      free(record);
   }
   return -1;
}

void doSelect(struct fieldSelect *fieldHead, struct _table *tableHead, struct where *whereHead) {
   //printf("REACHED SELECT FUNCTION @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
   FILE *fp;
   size_t nread;
   fp = fopen(tableHead->tableFileName, "rb");
   //printf("SELECT FROM THE TABLE FILE NAME OFFFFFFFFF: %s\n", tableHead->tableFileName);
   fseek(fp, 0, SEEK_END);
   int lengthOfFile = ftell(fp);
   int numRecords = 0;
   if (lengthOfFile == 0 || tableHead->reclen == 0) {
      printf("IS ZERO VALUE FOR LENGTH\n");
   } else {
      numRecords = lengthOfFile / tableHead->reclen;   
   }
   //numRecords--;
   fclose(fp);
   struct where *tempWhere = whereHead;
   for (int i = 0; i < numRecords; i++) { // get each record
      char *record = calloc(1, tableHead->reclen);
      getRecord(i, record, tableHead);
      tempWhere = whereHead;
      bool matches = true;
      while (tempWhere != NULL) {
         if (checkWhereMatch(record, tableHead->fields, tableHead->fieldcount, tempWhere) == false) {
            matches = false;
            break;
         }
         tempWhere = tempWhere->next;
      }
      if (matches) {
         //printf("HAS A MATCH RECORD TO PRINT YAY ############################################\n");
         showRecord(tableHead->fields, record, tableHead->fieldcount, fieldHead);
      }
      free(record);
   }
}

void addCommandToFile(struct fieldSelect *fieldHead, struct _table *tableA, char* fileName) {
   struct fieldSelect *fieldChecker = fieldHead; // pointer to front of fields linked list
   int tableAFieldCt = tableA->fieldcount;
   char addCommand[MAXINPUTLENGTH];
   strcpy(addCommand, "ADD "); // buffer to copy in add commands
   FILE *fp;
   fp = fopen(fileName, "a+"); // open file
   for (int i = 0; i < tableAFieldCt; i++) {
      //printf("THE TABLE NAME CHECKING: %s\n", tableA->fields[i].fieldName);
      fieldChecker = fieldHead;
      while (fieldChecker != NULL) {
         //printf("THE FIELD NAME COMPARING AGAINST: %s\n", fieldChecker->name);
         if (strcmp(fieldChecker->name, tableA->fields[i].fieldName)==0) {
            char fileAddCommand[MAXINPUTLENGTH];
            strcpy(fileAddCommand, addCommand); // adding ADD
            strcat(fileAddCommand, fieldChecker->name); // adding field name
            strcat(fileAddCommand, " "); // adding space
            strcat(fileAddCommand, tableA->fields[i].fieldType); // adding field type
            strcat(fileAddCommand, " "); // adding space
            char fieldLength[10];
            sprintf(fieldLength, "%d", tableA->fields[i].fieldLength);
            strcat(fileAddCommand, fieldLength);
            strcat(fileAddCommand, "\n");
            fputs(fileAddCommand, fp);
         }
         fieldChecker = fieldChecker->next;
      }
   }
   fclose(fp); // close file
}

int getNumberRecords(char *fileName, int recordLength) {
   FILE *fp;
   size_t nread;
   fp = fopen(fileName, "rb");
   fseek(fp, 0, SEEK_END);
   int lengthOfFile = ftell(fp);
   int numRecordsA = 0;
   if (lengthOfFile == 0 || recordLength == 0) {
      printf("IS ZERO VALUE FOR LENGTH\n");
   } else {
      numRecordsA = lengthOfFile / recordLength;
   }
   //numRecords--;
   fclose(fp);
   return numRecordsA;
}

bool checkMatchConstant(char *recordA, char *recordB, struct _table *tableA, struct _table *tableB, struct where *theWhere) {
   char checkRecordField[MAXINPUTLENGTH];
   int offset = 0;
   bool found = false;
   for (int i = 0; i < tableA->fieldcount; i++) {
      if (strcmp(tableA->fields[i].fieldName, theWhere->left)==0) { // found matching field name
         found = true;
         strncpy(checkRecordField, &recordA[offset], tableA->fields[i].fieldLength);
         if (strcmp(theWhere->right, checkRecordField)==0) {
            return true;
         } else {
            return false;
         }
      }
      offset += tableA->fields[i].fieldLength;
   }
   if (found == false) { // if not in table A, then check table B
      offset = 0;
      for (int i = 0; i < tableB->fieldcount; i++) {
         if (strcmp(tableB->fields[i].fieldName, theWhere->left)==0) { // found matching field name
            found = true;
            strncpy(checkRecordField, &recordB[offset], tableB->fields[i].fieldLength);
            if (strcmp(theWhere->right, checkRecordField)==0) {
               return true;
            } else {
               return false;
            }
         }
         offset += tableB->fields[i].fieldLength;
      }
   }
   return true; // didn't find
}

bool checkMatchVar(char *recordA, char *recordB, struct _table *tableA, struct _table *tableB, struct where *theWhere) {
   // assuming tableA is always on left side and tableB is on right
   bool isMatch = true;
   char checkRecordAField[MAXINPUTLENGTH];
   char checkRecordBField[MAXINPUTLENGTH];
   int offsetA = 0;
   int offsetB = 0;
   bool foundA = false;
   bool foundB = false;
   for (int i = 0; i < tableA->fieldcount; i++) {
      if (strcmp(tableA->fields[i].fieldName, theWhere->left)==0) { // found matching field name
         foundA = true;
         strncpy(checkRecordAField, &recordA[offsetA], tableA->fields[i].fieldLength);
      } else if (strcmp(tableA->fields[i].fieldName, theWhere->right)==0) { // check right side
         foundA = true;
         strncpy(checkRecordAField, &recordA[offsetA], tableA->fields[i].fieldLength);
      }
      offsetA += tableA->fields[i].fieldLength;
   }
   for (int i = 0; i < tableB->fieldcount; i++) {
      if (strcmp(tableB->fields[i].fieldName, theWhere->right)==0) {
         foundB = true;
         strncpy(checkRecordBField, &recordB[offsetB], tableB->fields[i].fieldLength);
      } else if (strcmp(tableB->fields[i].fieldName, theWhere->left)==0) {
         foundB = true;
         strncpy(checkRecordBField, &recordB[offsetB], tableB->fields[i].fieldLength);
      }
      offsetB += tableB->fields[i].fieldLength;
   }
   // only compare if both are found
   // if not both found, then return true
   if (foundA == true && foundB == true) {
      if (strcmp(checkRecordAField, checkRecordBField)==0) {
         isMatch = true;
      } else {
         isMatch = false;
      }
   } else {
      isMatch = true;
   }
   return isMatch;
}

bool checkMatchInequality(char *recordA, char *recordB, struct _table *tableA, struct _table *tableB, struct where *theWhere) {
   char checkRecordField[MAXINPUTLENGTH];
   int rightCompare = atoi(theWhere->right); // convert right field to int
   int offset = 0;
   bool found = false;
   for (int i = 0; i < tableA->fieldcount; i++) {
      if (strcmp(tableA->fields[i].fieldName, theWhere->left)==0) { // found matching field name
         found = true;
         strncpy(checkRecordField, &recordA[offset], tableA->fields[i].fieldLength);
         int recordCompare = atoi(checkRecordField); // convert record field to int
         if (theWhere->isGreaterThan) {
            if (recordCompare > rightCompare) {
               return true;
            } else {
               return false;
            }
         } else if (theWhere->isLessThan) {
            if (recordCompare < rightCompare) {
               return true;
            } else {
               return false;
            }
         }
      }
      offset += tableA->fields[i].fieldLength;
   }
   if (found == false) { // if not in table A, then check table B
      offset = 0;
      for (int i = 0; i < tableB->fieldcount; i++) {
         if (strcmp(tableB->fields[i].fieldName, theWhere->left)==0) { // found matching field name
            found = true;
            strncpy(checkRecordField, &recordB[offset], tableB->fields[i].fieldLength);
            int recordCompare = atoi(checkRecordField); // convert record field to int
            if (theWhere->isGreaterThan) {
               if (recordCompare > rightCompare) {
                  return true;
               } else {
                  return false;
               }
            } else if (theWhere->isLessThan) {
               if (recordCompare < rightCompare) {
                  return true;
               } else {
                  return false;
               }
            }
         }
         offset += tableB->fields[i].fieldLength;
      }
   }
   return true; // didn't find
}

bool checkMatch(char *recordA, char *recordB, struct _table *tableA, struct _table *tableB, struct where *theWhere) {
   bool isMatch = true;
   if (theWhere->isConstant) {  
      isMatch = checkMatchConstant(recordA, recordB, tableA, tableB, theWhere);
   } else if (theWhere->isLessThan == true || theWhere->isGreaterThan == true) {
      isMatch = checkMatchInequality(recordA, recordB, tableA, tableB, theWhere);
   } else {
      isMatch = checkMatchVar(recordA, recordB, tableA, tableB, theWhere); // may need to modify in future to be more flexible
      // what if the field is not in either table? return true
   }
   return isMatch;
}

void parseInsert(char *fileName, char *recordA, char *recordB, struct _table *tableA, struct _table *tableB, struct fieldSelect *fieldHead) {
   //printf("ENTERING PARSE INSERT ####################################################################################\n");
   FILE *fp;
   fp = fopen(fileName, "a+"); // open text file
   struct fieldSelect *tempSelect = fieldHead;
   char *fieldPrint;
   fieldPrint = calloc(1, MAXINPUTLENGTH);
   //char *resultPrint = calloc(1, MAXINPUTLENGTH); ///////////////////////////////////////////////////////////////////////////////// REMOVE
   int offset = 0;
   int seenCounter = 0;
   for (int i = 0; i < tableA->fieldcount; i++) { // get number of fields
      tempSelect = fieldHead;
      while (tempSelect != NULL) {
         if (strcmp(tempSelect->name, tableA->fields[i].fieldName)==0) {
            seenCounter++;
         }
         tempSelect = tempSelect->next;
      }
   }
   for (int i = 0; i < tableB->fieldcount; i++) { // get number of fields
      tempSelect = fieldHead;
      while (tempSelect != NULL) {
         if (strcmp(tempSelect->name, tableB->fields[i].fieldName)==0) {
            seenCounter++;
         }
         tempSelect = tempSelect->next;
      }
   }
   tempSelect = fieldHead;
   int insertCounter = 0;
   for (int i = 0; i < tableA->fieldcount; i++) {
      tempSelect = fieldHead;
      while (tempSelect != NULL) {
         if (strcmp(tempSelect->name, tableA->fields[i].fieldName)==0) {
            strncat(fieldPrint, &recordA[offset], tableA->fields[i].fieldLength);
            //if (tempSelect->isWhere == false) { ///////////////////////////////////////////////////////////////////// REMOVE
               //strncat(resultPrint, &recordA[offset], tableA->fields[i].fieldLength); /////////////////////////////////////////////// REMOVE
               //if (insertCounter != seenCounter - 1) { ////////////////////////////////////////////////////////////////////// REMOVE
                  //strcat(resultPrint, ","); /////////////////////////////////////////////////////////////////// REMOVE
               //} //////////////////////////////////////////////////////////////////////////////////////////// REMOVE
            //} /////////////////////////////////////////////////////////////////////////////////////////////////////////// REMOVE
            //printf("INSERT COUNTER IS: %d\n", insertCounter);
            //printf("SEEN COUNTER IS: %d\n", seenCounter);
            if (insertCounter < seenCounter-1) {
               strcat(fieldPrint, ",");
            }
            insertCounter++;
         }
         tempSelect = tempSelect->next;
      }
      offset += tableA->fields[i].fieldLength;
   }
   tempSelect = fieldHead;
   offset = 0;
   for (int i = 0; i < tableB->fieldcount; i++) {  
      tempSelect = fieldHead;
      while (tempSelect != NULL) {
         if (strcmp(tempSelect->name, tableB->fields[i].fieldName)==0) {
            strncat(fieldPrint, &recordB[offset], tableB->fields[i].fieldLength);
            //if (tempSelect->isWhere == false) { ///////////////////////////////////////////////////////////////////// REMOVE
               //strncat(resultPrint, &recordB[offset], tableB->fields[i].fieldLength); /////////////////////////////////////////////// REMOVE
               //if (insertCounter != seenCounter - 1) { ////////////////////////////////////////////////////////////////////// REMOVE
                  //strcat(resultPrint, ","); /////////////////////////////////////////////////////////////////// REMOVE
               //} //////////////////////////////////////////////////////////////////////////////////////////// REMOVE
            //} /////////////////////////////////////////////////////////////////////////////////////////////////////////// REMOVE
            //printf("INSERT COUNTER IS: %d\n", insertCounter);
            //printf("SEEN COUNTER IS: %d\n", seenCounter);
            if (insertCounter < seenCounter-1) {
               strcat(fieldPrint, ",");
            }
            insertCounter++;
         }
         tempSelect = tempSelect->next;
      }
      offset += tableB->fields[i].fieldLength;
   }
   // MAKE INSERT COMMAND
   char insertCommand[MAXINPUTLENGTH];
   strcpy(insertCommand, "INSERT INTO ");
   strcat(insertCommand, fileName);
   strcat(insertCommand, " ");
   strcat(insertCommand, fieldPrint); // the field to print
   strcat(insertCommand, "\n");
   fputs(insertCommand, fp);
   fclose(fp); // close file
   //////////////////////////////////////////////////////////////////////////////////////////// REMOVE LATER
   //printf("%s\n", resultPrint);
   //free(resultPrint);
   //////////////////////////////////////////////////////////////////////////////////////////// REMOVE LATER
   free(fieldPrint);
}

void parseAndInsertIndex(char *toParse, char *tempName, char *theIndex, struct fieldSelect *indexFields) {
   int numFields = 0;
   struct fieldSelect *temp = indexFields;
   while (temp != NULL) { // count number of fields
      numFields++;
      temp = temp->next;
   }
   FILE *fpTemp;
   fpTemp = fopen(tempName, "a+"); // open temp file
   char insertCommand[MAXINPUTLENGTH];
   strcpy(insertCommand, "INSERT INTO ");
   strcat(insertCommand, theIndex);
   strcat(insertCommand, " ");
   int numBytes = 0;
   for (int i = 0; i < numFields; i++) {
      char getIndexField[MAXINPUTLENGTH];
      strncpy(getIndexField, toParse + numBytes, 30);
      numBytes += 30; // increment the offset
      trimwhitespace(getIndexField);
      char *indexToken = getIndexField;
      while(isspace((unsigned char)(*indexToken))) indexToken++; // remove leading spaces
      //printf("THE STRING GOTTEN: %s\n", indexToken);
      strcat(insertCommand, indexToken); // concat to insert command string
      if (i < numFields - 1) {
         strcat(insertCommand, ",");
      }
   }
   strcat(insertCommand, "\n");
   fputs(insertCommand, fpTemp);
   fclose(fpTemp);
}

void createTempIndexFile(struct fieldSelect *theFields, char *indexName, struct _table *origTable) {
   FILE *fp;
   char indexFileTemp[MAXINPUTLENGTH]; // char array to store the file name
   strcpy(indexFileTemp, "TEMP");
   strcat(indexFileTemp, indexName);
   //printf("THE TEMP FILE NAME INDEX IS: %s\n", indexFileTemp);
   fp = fopen(indexFileTemp, "w+"); // create temp empty file for index
   ///////////////////////////////////////////////////// ADD CREATE TABLE TO FILE
   char createTableClause[200];
   strcpy(createTableClause, "CREATE TABLE ");
   strcat(createTableClause, indexName); // appending index name to CREATE TABLE
   strcat(createTableClause, "\n"); // adding new line
   fputs(createTableClause, fp); // inserting into file
   fclose(fp); // close file
   ////////////////////////////////////////////////////// ADD COMMANDS TO FILE
   addCommandToFile(theFields, origTable, indexFileTemp);
   /////////////////////////////////////////////////////// ADD END
   fp = fopen(indexFileTemp, "a+");
   fputs("END\n", fp); // add END keyword to file
   fclose(fp);
   /////////////////////////////////////////////////////// ADD INSERTS
   char sortedBuffer[MAXINPUTLENGTH]; // to read from sortme file
   fp = fopen("sortme.sorted", "r");
   char* statusIndex = fgets(sortedBuffer, sizeof(sortedBuffer)-1, fp); // read from file
   while (statusIndex != NULL) {
      //printf("THE TEMP INDEX FILE LINES: %s\n", sortedBuffer);
      parseAndInsertIndex(sortedBuffer, indexFileTemp, indexName, theFields); // parse the buffer and insert
      statusIndex = fgets(sortedBuffer, sizeof(sortedBuffer)-1, fp);
   }
   fclose(fp);
}

void createSortFile(struct _table *indexFromTable, struct fieldSelect *indexFields) {
   FILE *fp;
   fp = fopen("sortme.sortme", "w"); // open/create sortme file
   int numRecords = getNumberRecords(indexFromTable->tableFileName, indexFromTable->reclen);
   struct fieldSelect *tempField = indexFields; // temp pointer
   int bufferBytes = 0;
   for (int i = 0; i < numRecords; i++) {
      char *insertBuffer = calloc(1, MAXINPUTLENGTH);
      bufferBytes = 0;
      char *indexRecord = calloc(1, indexFromTable->reclen);
      getRecord(i, indexRecord, indexFromTable);
      tempField = indexFields; // setting back to head
      while (tempField != NULL) { // loop through fields linked list
         int eachOffset = 0;
         for (int j = 0; j < indexFromTable->fieldcount; j++) { // loop through all table fields
            if (strcmp(indexFromTable->fields[j].fieldName, tempField->name)==0) {
               //printf("THE INSERTING PIECE: %s\n", &indexRecord[eachOffset]);
               bufferBytes += sprintf(insertBuffer + bufferBytes, "%30s", &indexRecord[eachOffset]);
               //strncat(insertBuffer, &indexRecord[eachOffset], 40); // inserting each with 40 alotted spaces
            }
            eachOffset += indexFromTable->fields[j].fieldLength; // increment offset
         }
         tempField = tempField->next; // increment pointer
      }
      fprintf(fp, "%s\n", insertBuffer); // insert into sortme file
      free(indexRecord); // free record
      free(insertBuffer); // free insert buffer
   }
   fclose(fp); // close sortme file
}

struct _table* doJoin(struct fieldSelect *fieldHead, struct _table *tableHead, struct where *whereHead) {
   struct _table *tableStackHead = calloc(1, sizeof(struct _table)); // creating head for the stack
   tableStackHead->next = tableHead;
   FILE *fp;
   char commandFile[20];
   strcpy(commandFile, "TEMP");
   int joinCounter = 0;
   struct _table *tempTable = tableHead;
   while (tempTable->next != NULL) { // iterate through tables  
      char tempFileName[50]; // appending number to TEMP file name
      //printf("THE TEMP FILE COUNTER @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ %d\n", joinCounter);
      fflush(stdout);
      strcpy(tempFileName, commandFile);
      char tempNum[10];
      sprintf(tempNum, "%d", joinCounter);
      strcat(tempFileName, tempNum);
      // done appending number to TEMP file name
      fp = fopen(tempFileName, "w+"); // create empty file to write process commands
      /////////////////////// putting CREATE TABLE TEMPx into file
      char createTableClause[200];
      strcpy(createTableClause, "CREATE TABLE ");
      strcat(createTableClause, tempFileName); // apending TEMPx to CREATE TABLE
      strcat(createTableClause, "\n"); // adding new line
      fputs(createTableClause, fp); // inserting into file
      fclose(fp);
      ////////////////////// done putting CREATE TABLE command into file
      /////////////////////////// get the 2 tables and add necessary fields to file
      struct _table *tableA = tempTable; // pop the 2 tables
      struct _table *tableB = tempTable->next;
      tempTable = tempTable->next->next;
      tableStackHead->next = tempTable;
      // popped the 2 tables
      ////////////////////////////////////// ADD commands to file
      addCommandToFile(fieldHead, tableA, tempFileName);
      addCommandToFile(fieldHead, tableB, tempFileName);
      //////////////////////////////////////////////
      fp = fopen(tempFileName, "a+");
      fputs("END\n", fp); // add END keyword to file
      fclose(fp);
      //////////////////////////////
      // get number of records in each file
      int numRecordsA = getNumberRecords(tableA->tableFileName, tableA->reclen);
      int numRecordsB = getNumberRecords(tableB->tableFileName, tableB->reclen);
      //////////////////////////////////////////////////
      // loop through fields and check if matches the wheres and add INSERT COMMANDS
      struct where *whereTemp = whereHead; // where pointer
      for (int i = 0; i < numRecordsA; i++) {
         char *recordA = calloc(1, tableA->reclen);
         getRecord(i, recordA, tableA); // get record i from table a
         for (int j = 0; j < numRecordsB; j++) {
            bool isMatch = true;
            char *recordB = calloc(1, tableB->reclen);
            getRecord(j, recordB, tableB); // get record i from table b
            whereTemp = whereHead; // setting pointer back to head of where list
            while (whereTemp != NULL) {
               isMatch = checkMatch(recordA, recordB, tableA, tableB, whereTemp); //////////////// @@@@@@ CHECK THIS TO MAKE MORE FLEXIBLE FOR HWK 6
               if (isMatch == false) {
                  //printf("RECORD A: %s\n", recordA);
                    //printf("RECORD B: %s\n", recordB);
                  break;
               }
               whereTemp = whereTemp->next; // iterate through linked list
            }
            if (isMatch == true) { // if passes all where clauses
               //printf("RECORD A MATCH: %s\n", recordA);
               //printf("RECORD B MATCH: %s\n", recordB);
               parseInsert(tempFileName, recordA, recordB, tableA, tableB, fieldHead); ////////
            }
            free(recordB);
         } // end of recordsB loop
         free(recordA);
      } // end of recordsA loop     
      ////////////////////////////////////////////// push new table onto stack
      // processCommand on the file ///////////////////////////////////////////////////////////////////// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
      fp = fopen(tempFileName, "r");
      char initialTempBuffer[MAXINPUTLENGTH];
      char *hasMore = fgets(initialTempBuffer, MAXINPUTLENGTH-1, fp); // get first line of input from temp file
      int counter = 0;
      //printf("TEMP FILE BEFORE FILE READ!!!!!!!!!!!!!!!!!!!!!!!!! %d\n", joinCounter);
      fflush(stdout);
      if (hasMore == NULL) {
         //printf("HAS MORE IS NULL $$$$$$$$$$$$$$$$$$$$$$      %d\n", joinCounter);
         fflush(stdout);
      }
      while (hasMore != NULL) { // call processCommand
         //printf("INITIAL TEMP BUFFER: %s\n", initialTempBuffer);
         //printf("ABOUT TO CALL PROCESS COMMAND: %d\n", counter);
         fflush(stdout);
         processCommand(initialTempBuffer, fp, 0); // call process command on temp file
         hasMore = fgets(initialTempBuffer, MAXINPUTLENGTH-1, fp);
         counter++;
      }
      //printf("OUTSIDE OF WHILE LOOP #######################################\n");
      fflush(stdout);
      //printf("CALLED PROCESS COMMAND ##############################################\n");
      fclose(fp);
      ////////////////////////////////////////////////////////////////////////////////////////////////
      struct _table *newTable = calloc(1, sizeof(struct _table)); // push new table
      loadSchema(newTable, tempFileName, 0); //////////////////////////////////////////////////////////////////////////// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
      //printf("AFTER LOAD SCHEMA ###############################################\n");
      fflush(stdout);
      tableStackHead->next = newTable;
      newTable->next = tempTable;
      tempTable = newTable;
      ////////////////////////////////////////// pushed the new table onto stack 
      joinCounter++;
      //printf("END OF WHILE LOOP ########################################\n");
      fflush(stdout);
   } // end of table while loop
   return tempTable; // return the joined table
} // end of join function

void processCommand(char *theBuffer, FILE *inputFile, int showCommands) {
   //printf("IN PROCESS COMMAND\n");
   const char createCommand[20] = "CREATE TABLE";
   //const char loadCommand[20] = "LOAD TABLE";
   const char insertCommand[20] = "INSERT INTO";
   const char selectCommand[20] = "SELECT";
   const char dropCommand[20] = "DROP TABLE";
   const char andCommand[20] = "AND";
   const char indexCommand[20] = "CREATE INDEX";
   char *command;
   const char delim[2] = " ";
   if (strstr(theBuffer, createCommand)) {
      char theTableName[MAXINPUTLENGTH];
      //printf("CREATE TABLE COMMAND$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n");
      //command = strstr(theBuffer, createCommand);
      char *token = strtok(theBuffer, delim); // CREATE
      token = strtok(NULL, delim); // TABLE
      strcpy(theTableName, strtok(NULL, delim)); // copying table name into char array
      //printf("THE TABLE NAME IN CREATE TABLE: %s\n", theTableName);
      trimwhitespace(theTableName);
      createTable(theTableName, inputFile, showCommands); // call create table
      //raise(SIGINT);
      char completeName[MAXINPUTLENGTH];
      strcpy(completeName, theTableName);
      strcat(completeName, ".schema\0");
      //printf("THE SCHEMA FILE TO OPEN: %s\n", completeName);
      FILE *fp;
      size_t nread;
      fp = fopen(completeName, "r+");
      fseek(fp, 0, SEEK_SET);
      char eachLine[MAXINPUTLENGTH];
      fgets(eachLine, sizeof(eachLine)-1, fp);
      //printf("THE LINE FROM SCHEMA FILE TEST: %s\n", eachLine);
      fgets(eachLine, sizeof(eachLine)-1, fp);
      fclose(fp);
      //fseek(fp, 0, SEEK_END);
      //int lengthOfFile = ftell(fp);
      //printf("LENGTH OF FILE: %d\n", lengthOfFile);
      //printf("END OF CREATE TABLE @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
   }
   else if (strstr(theBuffer, insertCommand)) {
      //printf("INSERT INTO COMMAND#############################################\n");
      //printf("-------------------------------------------- THE BUFFER TO INSERT: %s\n", theBuffer);
      //printf("===> %s\n", theBuffer);
      char myTableN[MAXINPUTLENGTH];
      char dataInsert[MAXINPUTLENGTH];
      getTableName(myTableN, theBuffer, dataInsert); // get table name
      trimwhitespace(myTableN);
      //printf("THE TABLE NAME I GOT @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ %s\n", myTableN);
      //printf("REMAINING BUFFER @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ %s\n", dataInsert);
      //struct _table table; // initialize struct
      struct _table *table = calloc(1, sizeof(struct _table)); // free?
      loadSchema(table, myTableN, 1); // call load schema
      //printf("NUM FIELDS: %d\n", table.fieldcount);
      loadDatabase(table, dataInsert); // call load database
   } 
   else if (strstr(theBuffer, selectCommand)) {
      // create the 3 linked lists    
      struct fieldSelect *fieldHead = createFieldsList(theBuffer);
      struct fieldSelect *tempf = fieldHead;
      while (tempf != NULL) { // iterating through fields linked list
         //printf("THE FIELD IN FIELD LINKED LIST: %s\n", tempf->name);
         tempf = tempf->next;
      }
      
      char fromCommand[MAXINPUTLENGTH]; 
      fgets(fromCommand, sizeof(fromCommand)-1, stdin);  // get FROM clause
      printf("===> %s", fromCommand);
      struct _table *tableHead = addTablesList(fromCommand);
      struct _table *temph = tableHead;
      //fflush(stdout);
      while (temph != NULL) { // loop through linked list and load schema into struct
         //printf("TABLE LINKED LIST DETAILS: %s\n", temph->theName);
         //printf("TABLE LINKED LIST DETAILS: %s\n", temph->tableFileName);
         loadSchema(temph, temph->theName, 0);
         //fflush(stdout);
         temph = temph->next;
      }
      char wherePossCommand[MAXINPUTLENGTH];
      fgets(wherePossCommand, sizeof(wherePossCommand)-1, stdin); // get either END or WHERE clause
      bool isFirst = true;
      struct where *tempw;
      while (strcmp(wherePossCommand,"END\n")!=0) {
         printf("===> %s", wherePossCommand);
         char copyWhereCommand[MAXINPUTLENGTH];
         strncpy(copyWhereCommand, wherePossCommand, sizeof(wherePossCommand));
         if (isFirst) {
            tempw = createWhereList(wherePossCommand);
            if (strstr(copyWhereCommand, "<") == NULL && strstr(copyWhereCommand, ">") == NULL) {
               fieldHead = addFieldFromWhere(fieldHead, copyWhereCommand);
            }
            isFirst = false;
         } else {
            tempw = addWhereList(tempw, wherePossCommand); // append to where list
            if (strstr(copyWhereCommand, "<") == NULL && strstr(copyWhereCommand, ">") == NULL) {
               fieldHead = addFieldFromWhere(fieldHead, copyWhereCommand);
            }
         }
         fgets(wherePossCommand, sizeof(wherePossCommand)-1, stdin); // get either END or WHERE clause         
      }
      printf("===> %s", wherePossCommand);
      /////////////////////////////////////////////////////////// 3 linked lists created
      /*
      struct fieldSelect *newt = fieldHead;
      while (newt != NULL) {
         printf("TO TEST WHERE CREATE: %s\n", newt->name);
         //printf("TO TEST WHERE CREATE: %s\n", newt->right);
         //if (newt->isConstant == true) {
            //printf("IS TRUE\n");
         //} else {
            //printf("IS FALSE\n");
         //}
         newt = newt->next;
      }
      */
      struct _table *tempTable = tableHead;
      int tableCount = 0;
      while (tempTable != NULL) { // get count of number of tables
         tableCount++;
         tempTable = tempTable->next;
      }
      
      if (tableCount == 1) { // only one table
         if (*(tableHead->theName) == 'i' && tempw != NULL) {
            //printf("HAS AN I IN IT!\n");
            doBinarySelect(fieldHead, tableHead, tempw); 
         } else {
            doSelect(fieldHead, tableHead, tempw);
         }
      } else {
         //printf("ONE TABLE\n");
         fflush(stdout);
         struct _table *joinedTable = doJoin(fieldHead, tableHead, tempw);
         doSelect(fieldHead, joinedTable, NULL);
      }
   }
   else if (strstr(theBuffer, indexCommand)) {
      char bufferCopy[MAXINPUTLENGTH];
      strncpy(bufferCopy, theBuffer, strlen(theBuffer)+1);
      struct fieldSelect *indexFields = createIndexFields(bufferCopy);
      struct fieldSelect *temp = indexFields;
      while (temp != NULL) { 
         //printf("TEMP NAME: %s\n", temp->name);
         temp = temp->next;
      }
      //printf("GOT CREATE INDEX COMMAND\n");
      char *indexToken;
      const char delimIndex[2] = " ";
      indexToken = strtok(theBuffer, delimIndex); // CREATE
      indexToken = strtok(NULL, delimIndex); // INDEX
      indexToken = strtok(NULL, delimIndex); // INDEX NAME
      char indexName[MAXINPUTLENGTH]; // char array to store index name
      strcpy(indexName, indexToken); // strcpy to char array
      //printf("THE INDEX NAME IS: %s\n", indexName);
      char indexFromLine[MAXINPUTLENGTH];
      fgets(indexFromLine, sizeof(indexFromLine)-1, stdin); // get FROM TABLE
      printf("===> %s", indexFromLine); // print out FROM line
      indexToken = strtok(indexFromLine, delimIndex); // FROM
      indexToken = strtok(NULL, delimIndex); // TABLE NAME
      char indexFromTable[MAXINPUTLENGTH];
      strcpy(indexFromTable, indexToken);
      trimwhitespace(indexFromTable);
      //printf("THE INDEX FROM TABLE NAME IS: [%s]\n", indexFromTable);
      ////////////////////////////////////////////////////////////////////// PARSED ALL FIELDS
      struct _table *tableFromIndex = calloc(1, sizeof(struct _table));
      tableFromIndex->theName = calloc(1, MAXINPUTLENGTH);
      strncpy(tableFromIndex->theName, indexFromTable, sizeof(indexFromTable));
      loadSchema(tableFromIndex, tableFromIndex->theName, 0); // load the schema with the index table
      ///////////////////////////////////////////////////////////////////////// LOAD THE TABLE
      /////////////////////////////////////////////////////////////////////// CREATE SORTME.SORTME FILE
      createSortFile(tableFromIndex, indexFields);
      /////////////////////////////////////////////////////////////////////// SORT THE SORTME.SORTME FILE
      system("sort<sortme.sortme>sortme.sorted");
      /////////////////////////////////////////////////////////////////////// CREATE TEMP FILE FOR INDEX
      char theTempFileToProcess[MAXINPUTLENGTH];
      strcpy(theTempFileToProcess, "TEMP");
      strcat(theTempFileToProcess, indexName); // creating string for temp file name
      createTempIndexFile(indexFields, indexName, tableFromIndex);
      //printf("THE TEMP INDEX FILE IS: %s\n", theTempFileToProcess);
      ////////////////////////////////////////////////////////////////////// CALL PROCESS COMMAND ON IT
      FILE *fpTempIndex;
      fpTempIndex = fopen(theTempFileToProcess, "r");
      char indexTempBuffer[MAXINPUTLENGTH];
      char *hasMore = fgets(indexTempBuffer, MAXINPUTLENGTH-1, fpTempIndex); // get first line of input from temp file
      while (hasMore != NULL) { // call processCommand
         processCommand(indexTempBuffer, fpTempIndex, 0); // call process command on temp file
         hasMore = fgets(indexTempBuffer, MAXINPUTLENGTH-1, fpTempIndex);
      }
      ////////////////////////////////////////////////////////////////////// PRINT END
      char theEnd[MAXINPUTLENGTH];
      fgets(theEnd, sizeof(theEnd)-1, stdin); // GET END OF INDEX COMMAND
      printf("===> %s", theEnd); // print out END
   }
   else if (strstr(theBuffer, dropCommand)) {
      //printf("GOT DROP COMMAND\n");
      char *tokena;
      const char delima[2] = " ";
      tokena = strtok(theBuffer, delima); // DROP
      tokena = strtok(NULL, delima); // TABLE
      tokena = strtok(NULL, delima); // the table to be dropped
      char tableDrop[MAXINPUTLENGTH]; // char array to store name of table to be dropped
      //strncpy(tableDrop, tokena, sizeof(tokena));
      strcpy(tableDrop, tokena);
      //printf("TABLE TO BE DROPPED: %s\n", tableDrop);
      dropTable(tableDrop);
      //raise(SIGINT);
   }
} // END OF FUNCTION

int xmain() {
   static char buffer[MAXINPUTLENGTH];
   memset(buffer, 0, MAXINPUTLENGTH);
   printf("Welcome!\n");
   char *status = fgets(buffer, MAXINPUTLENGTH-1, stdin); 
   while (status != NULL) {
      trimwhitespace(buffer);
      if (strlen(buffer) < 5)
            break; // not a real command, CR/LF, extra line, etc.
      printf("===> %s\n", buffer);
      processCommand(buffer, stdin, 1);
      status = fgets(buffer, MAXINPUTLENGTH-1, stdin); 
    }     
   printf("Goodbye!\n");
   return 0;
}

int main() {
   static char buffer[MAXINPUTLENGTH];
   memset(buffer, 0, MAXINPUTLENGTH);
   //printf("Welcome!\n");
   char *status = fgets(buffer, MAXINPUTLENGTH-1, stdin); 
   while (status != NULL) {
      trimwhitespace(buffer);
      if (strlen(buffer) < 5)
            break; // not a real command, CR/LF, extra line, etc.
      //printf("===> %s\n", buffer);
      processCommand(buffer, stdin, 1);
      status = fgets(buffer, MAXINPUTLENGTH-1, stdin); 
    }     
   //printf("Goodbye!\n");
   printf("I attest that the code submitted here is the\nsame code that I will submit in Canvas\nand demonstrate to Dr. Shaffer\n");
   return 0;
}