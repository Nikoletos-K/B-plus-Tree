#ifndef UTILS_H_
#define UTILS_H_

#define CALL_BF(call){\
  \
  BF_ErrorCode code = call;\
  if(code != BF_OK){\
    BF_PrintError(code);\
    return AM_ERROR;\
  }\
}\

#define ACTIVE 1
#define EMPTY -1
#define MAX_OPEN_FILES 20
#define BUFFER 20
#define NO_SPLIT -1
#define MAX_SCANS 20

typedef enum typeOfNode{

    LEAF,INDEX

}typeOfNode;

typedef enum AM_Errors{

  AME_EOF=-1,AME_OK=0,AM_ERROR=-1,FILE_NOT_DESTROYED,
  FILE_ARRAY_FULL,FILE_NOT_IN_ARRAY,WRONG_SIZE,SCAN_ARRAY_FULL,
  NOT_FOUND,DESROY_INDEX_OPEN_SCAN,CLOSE_INDEX_OPEN_SCAN,SCAN_NOT_EXIST,
  FILE_NOT_CREATED,ARRAY_INDEX_OUT_OF_BOUNDS

}AM_Errors;

typedef struct headerData{

    char initChar;
    char keyType;
    int lengthOfKey;
    char valueType;
    int lengthOfValue;

    int maxTuples_OfLeafs;
    int maxTuples_OfIndexes;

    int leafPartition;
    int indexPartition;
    int index_isOdd;
    int leaf_isOdd;

    int treeRoot;

} headerData;

typedef struct openedScans_Data{

	int fileDesc;
	int block_id;
	int numOfRecord;
	int op;
	void * value;

}openedScans_Data;

openedScans_Data openedScans[MAX_SCANS];

typedef struct openFile_Data{


    int status;
    int fileDesc;
    char fileName[BUFFER];

    headerData * metadata;
    int treeRoot;

}openFile_Data;

openFile_Data openedFiles[MAX_OPEN_FILES];

typedef enum returnedValues{

	KEY_TYPE,L_KEY,VALUE_TYPE,L_VALUE,MAX_TUPLES_LEAFS,
	MAX_TUPLES_INDEXES,LEAF_PARTITION,INDEX_ODD,INDEX_PARTITION,
	LEAF_ODD,TREEROOT,FILE_ALREADY_EXISTS


}returnedValues;


typedef struct headerOfNode{

    typeOfNode type;

    int blockId;
    int numOfTuples;
    int nextBlock;

}headerOfNode;

typedef struct leafRecord {

    void * key;
    void * value;

} leafRecord;

int insert_toLeaf(char * data,leafRecord * lr,int fileDesc);
int insert_toIndex(int fileDesc,char * data,void * newKey,int leftPointer,int rightPointer);

int split_leafNode(int BlockId,int fileDesc,leafRecord * record,void ** returnedKey,int * newBlockId );
int split_indexNode(int BlockId,int fileDesc,void* entryKey,int rightPointer,void** returnedKey,int* newBlockId);

int compare(void * value1,void * value2,char type);

int insert_Recurcively(leafRecord * lr,void ** returnValue,int currentBlock,int fileDesc,int * newBlockId);

int findValue(int fileDesc,int BlockId,void *value,int op,int * block_id,int * numOfRecord);
#endif /* UTILS_H_ */
