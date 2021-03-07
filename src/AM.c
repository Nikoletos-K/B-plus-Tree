#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/AM.h"
#include "../include/bf.h"

int AM_errno = AME_OK;

void AM_Init() {

    BF_Init(LRU);  

    for(int i=0;i<MAX_OPEN_FILES;i++){  // initialize the global array of files with EMPtY(-1)
        openedFiles[i].status=EMPTY;
    }

    for(int i=0;i<MAX_SCANS;i++){      // initialize the global array of scans with EMPtY(-1)
        openedScans[i].fileDesc=EMPTY;
        openedScans[i].block_id=EMPTY;
        openedScans[i].numOfRecord=0;        

    }
}

void AM_PrintError(char *errString) {
    
    printf("ERROR: %s \n",errString);  

    switch(AM_errno){
        case AME_OK:
            printf("~ No error has occur\n");
            break;
        case AME_EOF:
            printf("~ End of file\n");
            break;
        case FILE_ARRAY_FULL:
            printf("~ No space in the array for a new open file \n");
            break;
        case WRONG_SIZE:
            printf("~ The size that have been entered for key or value isn't right \n");
            break;
        case SCAN_ARRAY_FULL:
            printf("~ Scan array is full , no other scan can be commited \n");
            break;
        case DESROY_INDEX_OPEN_SCAN:
            printf("~ Destroying of index failed , there's open scan at this moment \n");
            break;
        case CLOSE_INDEX_OPEN_SCAN:
            printf("~ Closing of index failed , there's open scan at this moment \n");
            break;
        case SCAN_NOT_EXIST:
            printf("~ Closing of a scan failed , not in the array \n");
            break;
        case FILE_NOT_CREATED:
            printf("~ File tried to be opened ,without being created \n");
            break;
        case ARRAY_INDEX_OUT_OF_BOUNDS:
            printf("~ Index inserted is not among 0 and MAX_SCANS or MAX_OPEN_FILES \n");
            break;
        case FILE_ALREADY_EXISTS:
            printf("~ File is already created! \n");
            break;


    }
}

int AM_CreateIndex(char *fileName,char attrType1,int attrLength1,char attrType2,int attrLength2) {

    /*---------- Checking for valid sizes --------------------*/
    switch(attrType1){
        case 'i':
            if(attrLength1!=4){
                AM_errno = WRONG_SIZE;
                return AM_ERROR;

            }
        case 'f':
            if(attrLength1!=4){
                AM_errno = WRONG_SIZE;
                return AM_ERROR;
            }
        case 'c':
            if(attrLength1<1 || attrLength1>255 ){
                AM_errno = WRONG_SIZE;
                return AM_ERROR;
            }
    }

    switch(attrType2){
        case 'i':
            if(attrLength2!=4){
                AM_errno = WRONG_SIZE;
                return AM_ERROR;
            }
        case 'f':
            if(attrLength2!=4){
                AM_errno = WRONG_SIZE;
                return AM_ERROR;
            }
        case 'c':
            if(attrLength2<1 || attrLength2>255 ){
                AM_errno = WRONG_SIZE;
                return AM_ERROR;
            }
    }

   
    BF_ErrorCode error =  BF_CreateFile(fileName);

    if(error==BF_FILE_ALREADY_EXISTS){          // if file exists already return
        AM_errno = FILE_ALREADY_EXISTS;
        return AM_ERROR;
    }

    int fd=0;
    CALL_BF(BF_OpenFile(fileName,&fd));

    /*---------------- First block initilaization ---------------*/

    BF_Block * firstBlock;
    BF_Block_Init(&firstBlock);
    CALL_BF(BF_AllocateBlock(fd,firstBlock));
    char * data = BF_Block_GetData(firstBlock);

    headerData hd;              // initialize the metadata - in the fisrt block of the file

    hd.initChar = 'F';
    hd.keyType = attrType1;

    hd.lengthOfKey = attrLength1;
    hd.valueType = attrType2;
    hd.lengthOfValue = attrLength2;


    hd.maxTuples_OfLeafs = ((BF_BLOCK_SIZE - sizeof(headerOfNode)) / (attrLength1+attrLength2)); 
    hd.maxTuples_OfIndexes = ((BF_BLOCK_SIZE - sizeof(headerOfNode) - sizeof(int)) / (attrLength1+sizeof(int))); 


    hd.leafPartition = hd.maxTuples_OfLeafs / 2;
    hd.leaf_isOdd  = hd.maxTuples_OfLeafs % 2;


    hd.indexPartition = hd.maxTuples_OfIndexes / 2;
    hd.index_isOdd  = hd.maxTuples_OfIndexes % 2;

    hd.treeRoot = EMPTY;

    memcpy(data,&hd,sizeof(headerData));
 
    BF_Block_SetDirty(firstBlock);
    CALL_BF(BF_UnpinBlock(firstBlock));
    BF_Block_Destroy(&firstBlock);

    /*---------------- END of - First block initilaization ---------------*/
    

    CALL_BF(BF_CloseFile(fd));

    return AME_OK;  
}

int AM_DestroyIndex(char *fileName) {

    /*----------- Checking if there are any scans to this file ------------------*/
    for(int i=0;i<MAX_OPEN_FILES;i++){
        if(strcmp(openedFiles[i].fileName,fileName)==0 && openedFiles[i].status==ACTIVE){
            for(int j=0;j<MAX_SCANS;j++){
                if(i == openedScans[j].fileDesc){
                    AM_errno = DESROY_INDEX_OPEN_SCAN;
                    return AM_ERROR;
                }
            }
        }
    }

    /*------- Then remove it ------------- */

    int error = remove(fileName);
    if(error!=0)
        AM_errno = FILE_NOT_DESTROYED;
    else 
        printf("File %s just destroyed \n",fileName);
    
    return AME_OK;
}

int AM_OpenIndex (char *fileName) {

    int fd;
    CALL_BF(BF_OpenFile(fileName,&fd));
    
    int BlocksCounter;
    CALL_BF(BF_GetBlockCounter(fd,&BlocksCounter));   

    /*--- Checking if this file doesn't exist ------------*/

    if(BlocksCounter==0){
        AM_errno = FILE_NOT_CREATED;
        return AM_ERROR;
    }

    /*--------------Take the frst block and read the metadata ---------*/

    BF_Block * firstBlock;
    BF_Block_Init(&firstBlock);
    CALL_BF(BF_GetBlock(fd,0,firstBlock));

    char * data = BF_Block_GetData(firstBlock);

    headerData * hd = malloc(sizeof(headerData));
    memcpy(hd,data,sizeof(headerData));

    /* ---------- Initializing the data of the array of files -------------*/
    int i=0;
    for(i=0;i<MAX_OPEN_FILES;i++){

        if(openedFiles[i].status == EMPTY){         // find the fisrt empty position in the array

            openedFiles[i].status = ACTIVE;
            openedFiles[i].fileDesc = fd;
            strcpy(openedFiles[i].fileName ,fileName);
            openedFiles[i].treeRoot = hd->treeRoot;
            openedFiles[i].metadata = hd;
            openedFiles[i].metadata->treeRoot = hd->treeRoot;

            
            CALL_BF(BF_UnpinBlock(firstBlock));
            BF_Block_Destroy(&firstBlock);

            return i;       // return the position of the array
        }
    }

    CALL_BF(BF_UnpinBlock(firstBlock));
    BF_Block_Destroy(&firstBlock);

    if(i==MAX_OPEN_FILES-1){            // if there are no empty positions return error
        AM_errno = FILE_ARRAY_FULL;
        return FILE_ARRAY_FULL;   
    }

    return AM_ERROR;
}

int AM_CloseIndex (int fileDesc) {

    /*-------- Checking for opened scans ------------*/
    for(int i=0;i<MAX_SCANS;i++){
        if(fileDesc == openedScans[i].fileDesc){
            AM_errno = CLOSE_INDEX_OPEN_SCAN;
            return AM_ERROR;
        }
    }


    /*----------- Closing file ------------*/

    openedFiles[fileDesc].status = EMPTY;
    openedFiles[fileDesc].fileDesc = EMPTY;
    openedFiles[fileDesc].treeRoot = EMPTY;
    free(openedFiles[fileDesc].metadata);
    openedFiles[fileDesc].metadata = NULL;

    CALL_BF(BF_CloseFile(fileDesc));
    
    return AME_OK;
}

int AM_InsertEntry(int fileDesc, void *value1, void *value2) {


    int fdArray = fileDesc;
    fileDesc = openedFiles[fdArray].fileDesc;

    /*------- Checking if this file not exists ---------------*/    
    int BlocksCounter;
    CALL_BF(BF_GetBlockCounter(fileDesc,&BlocksCounter));   
    if(BlocksCounter==0){
        AM_errno = FILE_NOT_CREATED;
        return AM_ERROR;
    }

    headerData * metadata = openedFiles[fdArray].metadata;

    /* ------- Putting values into a struct ------------*/
    int lengthOfValue = metadata->lengthOfValue;
    int lengthOfKey = metadata->lengthOfKey;
    int blockCounter;
    CALL_BF(BF_GetBlockCounter(fileDesc,&blockCounter));

    
    leafRecord newData ;
    newData.key = malloc(lengthOfKey);
    newData.value = malloc(lengthOfValue);

    memcpy(newData.key,value1,lengthOfKey);
    memcpy(newData.value,value2,lengthOfValue);



    if(blockCounter==1){    //if the only block is metadata ,create a leaf 

        BF_Block * newBlock;
        BF_Block_Init(&newBlock);
        CALL_BF(BF_AllocateBlock(fileDesc,newBlock));
        char * data = BF_Block_GetData(newBlock);

        int id;
        CALL_BF(BF_GetBlockCounter(fileDesc,&id));

        headerOfNode newhd;

        newhd.type = LEAF;
        newhd.blockId = id-1;
        newhd.numOfTuples = 1;
        newhd.nextBlock = EMPTY;

        memcpy(data,&newhd,sizeof(headerOfNode));
        data += sizeof(headerOfNode);

        memcpy(data,newData.key,metadata->lengthOfKey);  // save the key in the leaf
        data += metadata->lengthOfKey;
        memcpy(data,newData.value,metadata->lengthOfValue); // save the value in the leaf

        BF_Block_SetDirty(newBlock);
        CALL_BF(BF_UnpinBlock(newBlock));
        BF_Block_Destroy(&newBlock);

    }else{

        void * returnedValue = malloc(metadata->lengthOfKey);
        int currentBlock,blockid;
        if(openedFiles[fdArray].treeRoot == EMPTY)  // if treeroot has not been created yet
            currentBlock = blockCounter-1;    // there's only one block (leaf)
        else{   // else start with the root
            currentBlock=openedFiles[fdArray].treeRoot;
        }
        
        insert_Recurcively(&newData,&returnedValue,currentBlock,fdArray,&blockid);
        
        /* -------------- if a split has been done , create a new root ----------------------*/
        if(*((int *)returnedValue)!=NO_SPLIT){      // if returned value is not -1
            
            BF_Block * rootBlock;       // create a new block 
            BF_Block_Init(&rootBlock);
            CALL_BF(BF_AllocateBlock(fileDesc,rootBlock));
            
            int rootid;
            CALL_BF(BF_GetBlockCounter(fileDesc,&rootid));  // find the id of the block
            rootid--;

            openedFiles[fdArray].treeRoot = rootid;     // make the root
            openedFiles[fdArray].metadata->treeRoot = rootid;       // save the new root in metadata of the array of files


            char * data = BF_Block_GetData(rootBlock);      // innitialize its header
            headerOfNode newhd;
            newhd.type = INDEX;
            newhd.numOfTuples = 0;
            newhd.nextBlock = EMPTY;
            newhd.blockId = rootid;

            memcpy(data,&newhd,sizeof(headerOfNode));

            /*---------------- Informing metadata ------------------*/      
            BF_Block * firstBlock;
            BF_Block_Init(&firstBlock);
            CALL_BF(BF_GetBlock(fileDesc,0,firstBlock));
            char * fdata = BF_Block_GetData(firstBlock);
            headerData hd;
            memcpy(&hd,fdata,sizeof(headerData));            
            hd.treeRoot = rootid;                   // save the new root id in the first block, in case of a new open
            memcpy(fdata,&hd,sizeof(headerData));         
            BF_Block_SetDirty(firstBlock);
            CALL_BF(BF_UnpinBlock(firstBlock));
            BF_Block_Destroy(&firstBlock);
            /*------------------------------------------------------*/

            insert_toIndex(fdArray,data,returnedValue,currentBlock,blockid);        // insert the returned value in the new block

            BF_Block_SetDirty(rootBlock);
            CALL_BF(BF_UnpinBlock(rootBlock));
            BF_Block_Destroy(&rootBlock);
            free(returnedValue);

        }

    }

  return AME_OK;
}

int AM_OpenIndexScan(int fileDesc, int op, void *value) {

    
    int fdArray=fileDesc;
    fileDesc = openedFiles[fdArray].fileDesc;


    
    /*------- Checking if this file not exists ---------------*/    

    int BlocksCounter;
    CALL_BF(BF_GetBlockCounter(fileDesc,&BlocksCounter));   
 
    if(BlocksCounter==0){
        AM_errno = FILE_NOT_CREATED;
        return AM_ERROR;
    }

    int rootid = openedFiles[fdArray].treeRoot;

        
    if(rootid==EMPTY){  // case there is only one block -> LEAF
        CALL_BF(BF_GetBlockCounter(fileDesc,&rootid));
        rootid--; 
    }
    
    int i=0;
    for(i=0;i<MAX_SCANS;i++){

        if(openedScans[i].fileDesc==EMPTY){
            openedScans[i].fileDesc = fdArray;
            openedScans[i].op = op;
            openedScans[i].value = malloc(openedFiles[fdArray].metadata->lengthOfKey);
            break;
        }
   
    }

    int block_id,numOfRecord;

    /* -------- Call recurcive --------*/
    if(findValue(fdArray,rootid,value,op,&block_id,&numOfRecord)==AME_EOF){
        AM_errno = AME_EOF;
    }

    openedScans[i].block_id = block_id;
    openedScans[i].numOfRecord = numOfRecord;
    memcpy(openedScans[i].value,value,openedFiles[fdArray].metadata->lengthOfKey);
        
    if(i==MAX_SCANS)
        return SCAN_ARRAY_FULL;
    return i;
}

int findValue(int fileDesc,int BlockId,void *value,int op,int * block_id,int * numOfRecord){

    int fdArray=fileDesc;
    fileDesc = openedFiles[fdArray].fileDesc;

    /* --- Get block ----*/    
    BF_Block * Block;
    BF_Block_Init(&Block);
    CALL_BF(BF_GetBlock(fileDesc,BlockId,Block));
    char * data = BF_Block_GetData(Block);
    headerOfNode header;
    memcpy(&header,data,sizeof(headerOfNode));
    headerData* metadata = openedFiles[fdArray].metadata;
    data += sizeof(headerOfNode);

    /* ---------- Case 1: Block is leaf -----------------*/
    if(header.type == LEAF){

        int counter = 0;

        void * tempKey = malloc(metadata->lengthOfKey);
        memcpy(tempKey,data,metadata->lengthOfKey);

        int comparator,break_flag=0;
        
        /* ---- Search untill find the first that fits to this serach ---- */
        while(counter<header.numOfTuples){
            
            comparator = compare(value,tempKey,metadata->keyType);
            
            switch(op){

                case EQUAL:
                    if(comparator==0)
                        break_flag = 1;
                    break;
                case NOT_EQUAL:
                    if(comparator!=0)
                        break_flag = 1;
                    break;
                case LESS_THAN:
                    if(comparator>0)
                        break_flag = 1;
                    break;
                case GREATER_THAN:
                    if(comparator<0)
                        break_flag = 1;
                    break;
                case LESS_THAN_OR_EQUAL:
                    if(comparator>=0)
                        break_flag = 1;
                    break;
                case GREATER_THAN_OR_EQUAL:
                    if(comparator<=0)
                        break_flag = 1;
                    break;
            }

            if(break_flag)
                break;


            counter++;
            data += metadata->lengthOfKey+metadata->lengthOfValue;
            memcpy(tempKey,data,metadata->lengthOfKey);
            
        }
        *numOfRecord = counter;
        *block_id = BlockId;

        if(counter==header.numOfTuples-1){
            AM_errno = AME_EOF;
        }

        BF_Block_SetDirty(Block);
        CALL_BF(BF_UnpinBlock(Block));
        BF_Block_Destroy(&Block);


        return AME_OK;

    /* ---------- Case 2: Block is index ----------------- */

    }else{

        int counter=0;
        void * curPointer = malloc(metadata->lengthOfKey);
        data += sizeof(int);
        memcpy(curPointer,data,metadata->lengthOfKey);
        int comparator,break_flag=0;
        
        while(counter<header.numOfTuples ){

            comparator = compare(value,curPointer,metadata->keyType);
            switch(op){

                case EQUAL:
                    if(comparator<0)
                        break_flag = 1;
                    break;
                case NOT_EQUAL:
                    break_flag = 1;
                    break;
                case LESS_THAN:
                    break_flag = 1;
                    break;
                case GREATER_THAN:
                    if(comparator<0)
                        break_flag = 1;
                    break;
                case LESS_THAN_OR_EQUAL:
                    
                    break_flag = 1;
                    break;
                case GREATER_THAN_OR_EQUAL:
                    if(comparator<0)
                        break_flag = 1;
                    break;
            }

            if(break_flag)
                break;

            data += metadata->lengthOfKey+sizeof(int);
            memcpy(curPointer,data,metadata->lengthOfKey);                                
            counter++;
        }

        data -= sizeof(int);
        findValue(fdArray,*((int*) data),value,op,block_id,numOfRecord);    // recursion with this pointer

    }
    BF_Block_SetDirty(Block);
    CALL_BF(BF_UnpinBlock(Block));
    BF_Block_Destroy(&Block);

    return 0;
}

void * AM_FindNextEntry(int scanDesc) {

    /* ---- Checking for error in this array ------------*/
    if(scanDesc<0){
        AM_errno = ARRAY_INDEX_OUT_OF_BOUNDS;
        return NULL;
    }

    int fdArray = openedScans[scanDesc].fileDesc;
    int fileDesc =   openedFiles[fdArray].fileDesc;


    /*------- Checking if this file not exists ---------------*/        
    int BlocksCounter;
    BF_GetBlockCounter(fileDesc,&BlocksCounter);   
    if(BlocksCounter==0){
        AM_errno = FILE_NOT_CREATED;
        return NULL;
    }

    BF_Block * Block;
    BF_Block_Init(&Block);
    BF_GetBlock(fileDesc,openedScans[scanDesc].block_id,Block);
    char * data = BF_Block_GetData(Block);
    
    headerOfNode header;
    memcpy(&header,data,sizeof(headerOfNode));
    headerData* metadata = openedFiles[fdArray].metadata;
    
    data += sizeof(headerOfNode);
    data += openedScans[scanDesc].numOfRecord*(metadata->lengthOfKey+metadata->lengthOfValue);

    void * curKey = malloc(metadata->lengthOfKey);
    void * curValue = malloc(metadata->lengthOfValue);

    int comparator,counter;
    memcpy(curKey,data,metadata->lengthOfKey);
    
    data+= metadata->lengthOfKey;
    memcpy(curValue,data,metadata->lengthOfValue);
    data-= metadata->lengthOfKey;

    /* if we are at the end move next search to the next block */
    if(openedScans[scanDesc].numOfRecord == header.numOfTuples-1 &&  header.nextBlock!=EMPTY){ 
 
        openedScans[scanDesc].numOfRecord = 0;
        openedScans[scanDesc].block_id = header.nextBlock;
        
        BF_Block_SetDirty(Block);
        BF_UnpinBlock(Block);
        BF_Block_Destroy(&Block);

        return curValue;

    /* if we are at the end move next search to the next block, if empty return NULL */    
    }else if(openedScans[scanDesc].numOfRecord == header.numOfTuples && header.nextBlock==EMPTY){
        BF_UnpinBlock(Block);
        BF_Block_Destroy(&Block);
        AM_errno = AME_EOF;
        return NULL;
    
    }

    comparator = compare(openedScans[scanDesc].value,curKey,metadata->keyType);
   
    switch(openedScans[scanDesc].op){

        case EQUAL:

            if(comparator!=0){
                BF_UnpinBlock(Block);
                BF_Block_Destroy(&Block);
                AM_errno = AME_EOF;                
                return NULL;
            }
            break;
        
        case NOT_EQUAL:
            counter =  openedScans[scanDesc].numOfRecord;
            data += metadata->lengthOfValue+metadata->lengthOfKey;

            void * tempKey = malloc(metadata->lengthOfKey);
            memcpy(tempKey,data,metadata->lengthOfKey);
            
            while(counter<header.numOfTuples-1 && compare(openedScans[scanDesc].value,tempKey,metadata->keyType)==0){
                counter++;
                if(counter==header.numOfTuples-1){

                    openedScans[scanDesc].numOfRecord = -1;
                    openedScans[scanDesc].block_id = header.nextBlock;
                    if(header.nextBlock==EMPTY){
                        BF_UnpinBlock(Block);
                        BF_Block_Destroy(&Block);
                        AM_errno = AME_EOF;
                        return NULL;
                    }
                }
                data += metadata->lengthOfValue+metadata->lengthOfKey;
                memcpy(tempKey,data,metadata->lengthOfKey); 
                data += metadata->lengthOfKey;
                memcpy(curValue,data,metadata->lengthOfValue);
                data -= metadata->lengthOfKey;

            }

            if(openedScans[scanDesc].numOfRecord != -1)
                openedScans[scanDesc].numOfRecord = counter;


            break;

        case GREATER_THAN:
            counter =  openedScans[scanDesc].numOfRecord;

            while(counter<header.numOfTuples-1 && compare(openedScans[scanDesc].value,curKey,metadata->keyType)==0){
                counter++;
                if(counter==header.numOfTuples-1){
                    openedScans[scanDesc].numOfRecord = -1;
                    openedScans[scanDesc].block_id = header.nextBlock;
                    if(header.nextBlock==EMPTY){
                        BF_UnpinBlock(Block);
                        BF_Block_Destroy(&Block);
                        AM_errno = AME_EOF;
                        return NULL;
                    }
                }
                data += metadata->lengthOfValue+metadata->lengthOfKey;
                memcpy(curKey,data,metadata->lengthOfKey); 
                data += metadata->lengthOfKey;
                memcpy(curValue,data,metadata->lengthOfValue);
                data -= metadata->lengthOfKey;
            }
            if(openedScans[scanDesc].numOfRecord != -1)
                openedScans[scanDesc].numOfRecord = counter;
            break;
        
        case LESS_THAN:
        
            if(comparator<=0){
                BF_UnpinBlock(Block);
                BF_Block_Destroy(&Block);
                AM_errno = AME_EOF;
                return NULL;
            }
            break;

        
        case LESS_THAN_OR_EQUAL:
        
            if(comparator<0){
                AM_errno = AME_EOF;
                BF_UnpinBlock(Block);
                BF_Block_Destroy(&Block);
                return NULL;
            }
            break;
        
    
    }   

    openedScans[scanDesc].numOfRecord++;

    BF_Block_SetDirty(Block);
    BF_UnpinBlock(Block);
    BF_Block_Destroy(&Block);

    return curValue;  
}

int AM_CloseIndexScan(int scanDesc) {

    int BlocksCounter;
    CALL_BF(BF_GetBlockCounter(openedScans[scanDesc].fileDesc,&BlocksCounter));   
    
    if(BlocksCounter==0){
        AM_errno = FILE_NOT_CREATED;
        return AM_ERROR;
    }

    if(openedScans[scanDesc].fileDesc != EMPTY){

        openedScans[scanDesc].fileDesc=EMPTY;
        free(openedScans[scanDesc].value);

    }else{
        AM_errno = SCAN_NOT_EXIST;
        return SCAN_NOT_EXIST;
    }
    
    return AME_OK;
}

void AM_Close() {

    
    for(int i=0;i<MAX_SCANS;i++){
        if(openedScans[i].fileDesc!=EMPTY)
            AM_CloseIndexScan(i);

    }
    for(int i=0;i<MAX_SCANS;i++){
        if(openedFiles[i].status==ACTIVE)
            AM_CloseIndex(i);
    }

    BF_Close();
}

int compare(void * value1,void * value2,char type){

    float floatRes;
    switch(type){

        case 'i':
            return (*((int *) value1) - *((int *)value2));
        case 'f':
            floatRes = (*((float *) value1) - *((float *)value2));

            if(floatRes>0.0)
                return 1;
            else if(floatRes<0.0)
                return -1;
            else
                return 0;

        case 'c':
            return strcmp((char *) value1,(char*) value2);    
    
    }
    return AM_ERROR;
}

int insert_Recurcively(leafRecord * lr,void ** returnValue,int currentBlock,int fileDesc,int * newBlockId){

    int fdArray = fileDesc;
    fileDesc = openedFiles[fdArray].fileDesc;
    BF_Block * curBlock;
    BF_Block_Init(&curBlock);
    CALL_BF(BF_GetBlock(fileDesc,currentBlock,curBlock));


    char * data = BF_Block_GetData(curBlock);
    char * startOfData = data;
    headerOfNode newhd;
    memcpy(&newhd,data,sizeof(headerOfNode));        
    data += sizeof(headerOfNode);

    headerData * metadata = openedFiles[fdArray].metadata;

    if(newhd.type==LEAF){

        if(newhd.numOfTuples<metadata->maxTuples_OfLeafs){      // if there's space in the leaf

            int value = NO_SPLIT;
            memcpy(*returnValue,&value,sizeof(int));        // return no split(-1)
            insert_toLeaf(startOfData,lr,fdArray);

        }else{      // if there is no space for another record

            void * returnedKey = malloc(metadata->lengthOfKey);
            int blockid;
            split_leafNode(currentBlock,fdArray,lr,&returnedKey,&blockid);   // split the current block

            if(openedFiles[fdArray].treeRoot==EMPTY){   // if there is no root create one

                BF_Block * rootBlock;
                BF_Block_Init(&rootBlock);
                CALL_BF(BF_AllocateBlock(fileDesc,rootBlock));
                
                int rootid;
                CALL_BF(BF_GetBlockCounter(fileDesc,&rootid));
                rootid--;
                openedFiles[fdArray].treeRoot = rootid;

                /*---------------- Informing metadata ------------------*/
                BF_Block * firstBlock;
                BF_Block_Init(&firstBlock);
                CALL_BF(BF_GetBlock(fileDesc,0,firstBlock));
                char * fdata = BF_Block_GetData(firstBlock);
                headerData hd;
                memcpy(&hd,fdata,sizeof(headerData));            
                hd.treeRoot = rootid;
                memcpy(fdata,&hd,sizeof(headerData));         
                BF_Block_SetDirty(firstBlock);
                CALL_BF(BF_UnpinBlock(firstBlock));
                BF_Block_Destroy(&firstBlock);
                /*------------------------------------------------------*/
                
                char * data = BF_Block_GetData(rootBlock);
                headerOfNode newhd;
                newhd.type = INDEX;
                newhd.numOfTuples = 0;
                newhd.nextBlock = EMPTY;
                newhd.blockId = rootid;

                memcpy(data,&newhd,sizeof(headerOfNode));

                insert_toIndex(fdArray,data,returnedKey,currentBlock,blockid);  // insert the new key of the split, in the root

                BF_Block_SetDirty(rootBlock);
                CALL_BF(BF_UnpinBlock(rootBlock));
                BF_Block_Destroy(&rootBlock);
                int value = NO_SPLIT;
                memcpy(*returnValue,&value,sizeof(int));
                *newBlockId = blockid;

            }else{

                memcpy(*returnValue,returnedKey,metadata->lengthOfKey);
                *newBlockId = blockid;                
            }

        }

    }else{      // if it is INDEX

        
        int counter=0;
        void * curPointer = malloc(metadata->lengthOfKey);
        data += sizeof(int);
        memcpy(curPointer,data,metadata->lengthOfKey);

        while(counter<newhd.numOfTuples && compare(lr->key,curPointer,metadata->keyType)>=0){   // check if the insert key is greater than the current key
            data += metadata->lengthOfKey+sizeof(int);  // if it is continue searching the index
            if(counter==newhd.numOfTuples-1)
                break;
            memcpy(curPointer,data,metadata->lengthOfKey);                                
            counter++;
        }

        data -= sizeof(int); // go back to the pointer
        void * newReturnedValue = malloc(metadata->lengthOfKey);
        int newBlock;
        insert_Recurcively(lr,&newReturnedValue,(*(int *) data),fdArray,&newBlock); // now go and check at the next suitable the block 

        if(*((int *)newReturnedValue) != NO_SPLIT){     // if the block down split

            if(newhd.numOfTuples < metadata->maxTuples_OfIndexes){  // and there's space in this block
                int value = NO_SPLIT;
                memcpy(*returnValue,&value,sizeof(int));    // return NO_SPLIT
                insert_toIndex(fdArray,startOfData,newReturnedValue,EMPTY,newBlock);    //and add the returned value 
            }
            else                
                split_indexNode(currentBlock,fdArray,newReturnedValue,newBlock,returnValue,newBlockId); // if there's no space split this index
            

        }else{  
            int value = NO_SPLIT;   // if the block down didn't split return NO_SPLIT
            memcpy(*returnValue,&value,sizeof(int));

        }
        free(newReturnedValue);
    }
    
    BF_Block_SetDirty(curBlock);
    CALL_BF(BF_UnpinBlock(curBlock));
    BF_Block_Destroy(&curBlock);

    return AME_OK;
}

int split_leafNode(int BlockId,int fileDesc,leafRecord * record,void ** returnedKey,int * newBlockId ){

    int fdArray=fileDesc;
    fileDesc = openedFiles[fdArray].fileDesc;

    BF_Block * block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fileDesc,BlockId,block));
    char * data = BF_Block_GetData(block);
    char * startOf_data = data;

    headerOfNode header;
    memcpy(&header,data,sizeof(headerOfNode)); 
    headerData * metadata = openedFiles[fdArray].metadata;  
    void * middleKey = malloc(metadata->lengthOfKey);
    
    int partition = metadata->leafPartition;
    

    BF_Block * newBlock;            // create the new block needed
    BF_Block_Init(&newBlock);
    CALL_BF(BF_AllocateBlock(fileDesc,newBlock));
    char * newdata = BF_Block_GetData(newBlock);
    char * startOf_newData = newdata;
    CALL_BF(BF_GetBlockCounter(fileDesc,newBlockId));
    
    (*newBlockId)--;
    headerOfNode newhd;
    newhd.type = LEAF;
    newhd.blockId = *newBlockId;
    newhd.nextBlock = header.nextBlock;
    header.nextBlock = *newBlockId;
    newhd.numOfTuples=0;
   
    data += sizeof(headerOfNode) + partition*(metadata->lengthOfKey+metadata->lengthOfValue);
    newdata += sizeof(headerOfNode);

    memcpy(middleKey,data,metadata->lengthOfKey);           // find the key in the middle of the block 
    
    if(compare(record->key,middleKey,metadata->keyType)<0){     // if the insert key is less than the middle
    
        // copy all the records right from the middle key to the new block
        memcpy(newdata,data,(metadata->maxTuples_OfLeafs - partition)*(metadata->lengthOfKey+metadata->lengthOfValue));

        header.numOfTuples = partition;  // change the num of records in both blocks
        newhd.numOfTuples = metadata->maxTuples_OfLeafs - partition;
        memcpy(startOf_data,&header,sizeof(headerOfNode));
        memcpy(startOf_newData,&newhd,sizeof(headerOfNode));

        insert_toLeaf(startOf_data,record,fdArray);
    
    }else{

        // if number of iserted data is odd 
        if(metadata->leaf_isOdd){

            // increment data to the next value
            data += metadata->lengthOfKey+metadata->lengthOfValue;

            memcpy(newdata,data,partition*(metadata->lengthOfKey+metadata->lengthOfValue));
            
            header.numOfTuples = partition+1; // 1 bigger in the already existant block
            newhd.numOfTuples = partition;  // to this block will be inserted the value

            memcpy(startOf_data,&header,sizeof(headerOfNode));
            memcpy(startOf_newData,&newhd,sizeof(headerOfNode));

            insert_toLeaf(startOf_newData,record,fdArray);   // insert to the new block
     

        }else{
        // else number is even 
            memcpy(newdata,data,(metadata->maxTuples_OfLeafs - partition)*(metadata->lengthOfKey+metadata->lengthOfValue));
            
            header.numOfTuples = partition;
            newhd.numOfTuples = partition; // equal number of values to the blocks

            memcpy(startOf_data,&header,sizeof(headerOfNode));
            memcpy(startOf_newData,&newhd,sizeof(headerOfNode));
            insert_toLeaf(startOf_newData,record,fdArray);  // insert to the new block

        }
    }

    startOf_newData += sizeof(headerOfNode);
    
    
    memcpy(*returnedKey,startOf_newData,metadata->lengthOfKey);

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    BF_Block_SetDirty(newBlock);
    CALL_BF(BF_UnpinBlock(newBlock));
    BF_Block_Destroy(&newBlock);
    
    return AME_OK;
}

int split_indexNode(int BlockId,int fileDesc,void* entryKey,int rightPointer,void** returnedKey,int* newBlockId){

    int fdArray = fileDesc;
    fileDesc = openedFiles[fdArray].fileDesc;

    /* initializing the block */
    BF_Block * block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fileDesc,BlockId,block));
    char * data = BF_Block_GetData(block);
    char * startOf_data = data;

    /* reading the header */
    headerOfNode header;
    memcpy(&header,data,sizeof(headerOfNode));   
    headerData * metadata = openedFiles[fdArray].metadata;

    int partition = metadata->indexPartition;
    
    /* left and right value of the partition */
    void * leftValue = malloc(metadata->lengthOfKey);
    void * rightValue= malloc(metadata->lengthOfKey);

    /* new block of index */
    BF_Block * newBlock;
    BF_Block_Init(&newBlock);
    CALL_BF(BF_AllocateBlock(fileDesc,newBlock));
    char * newdata = BF_Block_GetData(newBlock);
    char * startOf_newData = newdata;

    /* initialization of block */
    CALL_BF(BF_GetBlockCounter(fileDesc,newBlockId));
    (*newBlockId)--;
    headerOfNode newhd;
    newhd.type = INDEX;
    newhd.nextBlock = EMPTY;
    newhd.blockId = *newBlockId;
    newhd.numOfTuples = 0;
    memcpy(startOf_newData,&newhd,sizeof(headerOfNode));

    data += sizeof(headerOfNode) + partition*(metadata->lengthOfKey + sizeof(int)) + sizeof(int);
    newdata +=sizeof(headerOfNode);
    
    if(metadata->index_isOdd){

        memcpy(leftValue,data,metadata->lengthOfKey);
        data += metadata->lengthOfKey + sizeof(int);
        memcpy(rightValue,data,metadata->lengthOfKey);

    }else{
        memcpy(rightValue,data,metadata->lengthOfKey);
        data -= metadata->lengthOfKey + sizeof(int);
        memcpy(leftValue,data,metadata->lengthOfKey);
        data += metadata->lengthOfKey + sizeof(int);
        
    } 

    /* ------------- 3 casess -------------*/
    if(compare(entryKey,leftValue,metadata->keyType)<0){    // if key to be entrered is smaller than the left value of the partition
        memcpy(*returnedKey,leftValue,metadata->lengthOfKey);   // return value will be the left
        data -= sizeof(int);
        
        if(metadata->index_isOdd){
            header.numOfTuples = partition;
            newhd.numOfTuples = metadata->maxTuples_OfIndexes-partition-1;
            memcpy(newdata,data,(metadata->maxTuples_OfIndexes-partition-1)*(sizeof(int)+metadata->lengthOfKey)+sizeof(int));
        }else{
            header.numOfTuples = partition-1;
            newhd.numOfTuples = partition;
            data-=sizeof(int);
            memcpy(newdata,data,partition*(sizeof(int)+metadata->lengthOfKey)+sizeof(int));
        }
        memcpy(startOf_data,&header,sizeof(headerOfNode));
        memcpy(startOf_newData,&newhd,sizeof(headerOfNode));
        char* temp = startOf_data;
        temp += sizeof(headerOfNode);
        int leftPointer;
        memcpy(&leftPointer,temp,sizeof(int)); 
        insert_toIndex(fdArray,startOf_data,entryKey,leftPointer,rightPointer);     // insert to the old block

    }else if(compare(entryKey,rightValue,metadata->keyType)>0){     // if value is bigger than the right value 

        memcpy(*returnedKey,rightValue,metadata->lengthOfKey);  // return right value
        data += metadata->lengthOfKey;
        if(metadata->index_isOdd){
            newhd.numOfTuples = metadata->maxTuples_OfIndexes-partition-2;
            header.numOfTuples = partition + 1;
            memcpy(newdata,data,(metadata->maxTuples_OfIndexes-partition-2)*(sizeof(int)+metadata->lengthOfKey)+sizeof(int));
        }else{
            newhd.numOfTuples = partition-1;
            header.numOfTuples = partition;
            memcpy(newdata,data,(partition-1)*(sizeof(int)+metadata->lengthOfKey)+sizeof(int));
        }
        memcpy(startOf_data,&header,sizeof(headerOfNode));
        memcpy(startOf_newData,&newhd,sizeof(headerOfNode));
        char* temp = data;
        int leftPointer;
        memcpy(&leftPointer,temp,sizeof(int)); 
        insert_toIndex(fdArray,startOf_newData,entryKey,leftPointer,rightPointer);


    }else if(compare(entryKey,leftValue,metadata->keyType)>0 && compare(entryKey,rightValue,metadata->keyType)<0){
        memcpy(*returnedKey,entryKey,metadata->lengthOfKey);
        
        // if value is among left and right the return the entry value to the above level 

        if(metadata->index_isOdd){
            newhd.numOfTuples = partition;
            header.numOfTuples = partition + 1;
        }else{
            newhd.numOfTuples = partition;
            header.numOfTuples = partition;
        }
        memcpy(newdata,&rightPointer,sizeof(int));
        newdata += sizeof(int);
        memcpy(newdata,data,partition*(sizeof(int)+metadata->lengthOfKey));
    
        memcpy(startOf_data,&header,sizeof(headerOfNode));
        memcpy(startOf_newData,&newhd,sizeof(headerOfNode));

    }

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    BF_Block_SetDirty(newBlock);
    CALL_BF(BF_UnpinBlock(newBlock));
    BF_Block_Destroy(&newBlock);

    return AME_OK;
}

int insert_toLeaf(char * data,leafRecord * lr,int fileDesc){

    int fdArray=fileDesc;
    fileDesc = openedFiles[fdArray].fileDesc;

    headerOfNode blockhead;
    memcpy(&blockhead,data,sizeof(headerOfNode));
    char * startOfData = data;
    data += sizeof(headerOfNode);
    headerData * metadata = openedFiles[fdArray].metadata;
    char * rdata,* tempdata = data;
    int counter=0;

    void * tempKey = malloc(metadata->lengthOfKey);
    memcpy(tempKey,tempdata,metadata->lengthOfKey); 

    // loop untill finding the right position    

    while(counter<blockhead.numOfTuples && compare(lr->key,tempKey,metadata->keyType)>=0){
    
        counter++;
        tempdata += metadata->lengthOfKey+metadata->lengthOfValue;
        memcpy(tempKey,tempdata,metadata->lengthOfKey);

    }

    // when you find it , move the data to make space 
    rdata = tempdata;
    rdata += metadata->lengthOfKey+metadata->lengthOfValue;
    memcpy(rdata,tempdata,(metadata->lengthOfKey+metadata->lengthOfValue)*(blockhead.numOfTuples-counter));
    memcpy(tempdata,lr->key,metadata->lengthOfKey);
    tempdata += metadata->lengthOfKey;
    memcpy(tempdata,lr->value,metadata->lengthOfValue);
    // and finally enter that value

    blockhead.numOfTuples++;
    memcpy(startOfData,&blockhead,sizeof(headerOfNode));

    return AME_OK;
}

int insert_toIndex(int fileDesc, char * data,void * newKey,int leftPointer,int rightPointer){

    int fdArray = fileDesc;
    fileDesc = openedFiles[fdArray].fileDesc;

    headerOfNode blockhead;
    char * rdata,* tempdata = data,*startOfData = data;
    memcpy(&blockhead,data,sizeof(headerOfNode));
    tempdata+= sizeof(headerOfNode);
    headerData * metadata = openedFiles[fdArray].metadata;
    int counter=0;
    void * tempKey  = malloc(metadata->lengthOfKey);

    /* if leftpinter is not empty , that means that we will have to initialize him */
    if(leftPointer != EMPTY){

        /* in case we have an empty block and will be at the begining */
        if(blockhead.numOfTuples==0){

            memcpy(tempdata,&leftPointer,sizeof(int));
            tempdata += sizeof(int);
            memcpy(tempdata,newKey,metadata->lengthOfKey);
            tempdata += metadata->lengthOfKey;
            memcpy(tempdata,&rightPointer,sizeof(int));

            blockhead.numOfTuples++;
            memcpy(startOfData,&blockhead,sizeof(headerOfNode));

            return AME_OK;
        
        /* and in case that the new value is smaller than the firts of the bock so it is going to take it's place */
        }else{

            tempdata += sizeof(int);
            void * firstKey = malloc(metadata->lengthOfKey);
            memcpy(firstKey,tempdata,metadata->lengthOfKey);

            if(compare(newKey,firstKey,metadata->keyType)<0){

                rdata=tempdata;
                rdata += sizeof(int)+metadata->lengthOfKey;
                memcpy(rdata,tempdata,blockhead.numOfTuples*(sizeof(int)+metadata->lengthOfKey));
                tempdata -= sizeof(int);
                memcpy(tempdata,&leftPointer,sizeof(int));
                tempdata += sizeof(int);
                memcpy(tempdata,newKey,metadata->lengthOfKey);
                tempdata += metadata->lengthOfKey;
                memcpy(tempdata,&rightPointer,sizeof(int));
                blockhead.numOfTuples++;
                memcpy(startOfData,&blockhead,sizeof(headerOfNode));

                return AME_OK;

            /* else just call again itself with empty leftpointer */
            }else{
                insert_toIndex(fdArray,data,newKey,EMPTY,rightPointer);
                return AME_OK;
            }

        }
    }


    /* same method as inserting leaf values */
    tempdata += sizeof(int);
    memcpy(tempKey,tempdata,metadata->lengthOfKey);

    while(counter<blockhead.numOfTuples && compare(newKey,tempKey,metadata->keyType)>=0 ){
      
        counter++;
        tempdata += metadata->lengthOfKey + sizeof(int);
        memcpy(tempKey,tempdata,metadata->lengthOfKey);


    }

    rdata = tempdata;

    rdata += (metadata->lengthOfKey + sizeof(int));

    
    memcpy(rdata,tempdata,(blockhead.numOfTuples-counter)*((metadata->lengthOfKey)+sizeof(int)));

    memcpy(tempdata,newKey,metadata->lengthOfKey);
    tempdata += metadata->lengthOfKey;
    memcpy(tempdata,&rightPointer,sizeof(int));

    blockhead.numOfTuples++;
    memcpy(startOfData,&blockhead,sizeof(headerOfNode));

    return AME_OK;
}

