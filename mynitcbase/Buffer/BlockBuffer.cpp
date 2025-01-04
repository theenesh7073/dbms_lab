
#include "BlockBuffer.h"
//#include "Disk.h"
#include <cstdlib>
#include <cstring>
#include<iostream>

BlockBuffer::BlockBuffer(int blockNum) {
    this->blockNum = blockNum; // initialize this.blockNum with the argument
}

BlockBuffer::BlockBuffer(char blockType)
{
    int blockTypeNum;
    if (blockType == 'R')
    {
        blockTypeNum = REC;
    }
    else if (blockType == 'I')
    {
        blockTypeNum = IND_INTERNAL;
    }
    else if (blockType == 'L')
    {
        blockTypeNum = IND_LEAF;
    }
    else
    {
        blockTypeNum = UNUSED_BLK;
    }
    int blockNum = getFreeBlock(blockTypeNum);

    this->blockNum = blockNum;
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
        return;
}


RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}
RecBuffer::RecBuffer() : BlockBuffer('R'){}



int BlockBuffer::getHeader(struct HeadInfo *head) {
    //unsigned char buffr[BLOCK_SIZE];
    // read the block at this.blockNum into the buffer
    //Disk::readBlock(buffr, this->blockNum);
    unsigned char *bufferptr;
	int ret=loadBlockAndGetBufferPtr(&bufferptr);
	if(ret!=SUCCESS)
	{
		return ret;
	}
	unsigned char buffr[BLOCK_SIZE];
    // read the block at this.blockNum into the buffer
    Disk::readBlock(buffr, this->blockNum);
    // populate the HeadInfo fields
    memcpy(&head->numSlots, bufferptr + 24, 4);
    memcpy(&head->numEntries, bufferptr + 16, 4);
    memcpy(&head->numAttrs, bufferptr + 20, 4);
    memcpy(&head->rblock, bufferptr + 12, 4);
    memcpy(&head->lblock, bufferptr + 8, 4);

    return SUCCESS;
}

int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
    struct HeadInfo head;
    

    // get the header using this.getHeader() function
    this->getHeader(&head);
    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;
    unsigned char *bufferptr;
	int ret=loadBlockAndGetBufferPtr(&bufferptr);
	if(ret!=SUCCESS)
	{
		return ret;
	}

    // read the block at this.blockNum into a buffer
    unsigned char buffer[BLOCK_SIZE];
    Disk::readBlock(buffer, this->blockNum);

    
    int recordSize = attrCount * ATTR_SIZE;
    int slotMapSize = slotCount; 
    int offset = HEADER_SIZE + slotMapSize + (recordSize * slotNum);

   
    memcpy(rec, bufferptr + offset, recordSize);

    return SUCCESS;
}

/*int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) 
{
  
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  if (bufferNum == E_BLOCKNOTINBUFFER) {
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND;
    }

    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
  *buffPtr = StaticBuffer::blocks[bufferNum];

  return SUCCESS;
}*/


int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {
  /* check whether the block is already present in the buffer
    using StaticBuffer.getBufferNum() */
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  if (bufferNum != E_BLOCKNOTINBUFFER) {
    for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
      StaticBuffer::metainfo[bufferIndex].timeStamp++;
    }
    StaticBuffer::metainfo[bufferNum].timeStamp = 0;
  } else {

    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND; // the blockNum is invalid
    }

    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }
  *buffPtr=StaticBuffer::blocks[bufferNum];
  return SUCCESS;
}


int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
    unsigned char *bufferPtr;
    int bufferNum = BlockBuffer::loadBlockAndGetBufferPtr(&bufferPtr);
    
    if (bufferNum != SUCCESS) {
        return bufferNum;
    }

    HeadInfo head;
    BlockBuffer::getHeader(&head);
  
    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;
    
    if (slotNum > slotCount || slotNum < 0) {
        return E_OUTOFBOUND;
    }

    int recordSize = attrCount * ATTR_SIZE;
    unsigned char *slotPointer = bufferPtr + (32 + slotCount + (recordSize * slotNum));

    memcpy(slotPointer, rec, recordSize);

    int ret = StaticBuffer::setDirtyBit(this->blockNum);
    
    if (ret != SUCCESS) {
        std::cout << "something wrong with the setDirty function";
    }

    return SUCCESS;
}


int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;
  // get the starting address of the buffer containing the block using
  // loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }
  struct HeadInfo head;
  BlockBuffer::getHeader(&head);
  // get the header of the b
  int slotCount = head.numSlots; /* number of slots in block from header */
  // get a pointer to the beginning of the slotmap in memory by offsetting
  // HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;
  // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
  memcpy(slotMap, slotMapInBuffer, slotCount);

  return SUCCESS;
}



int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
    double diff;

    // If the attribute type is STRING
    if (attrType == STRING) {
        diff = strcmp(attr1.sVal, attr2.sVal);
    } 
    // If the attribute type is NUMERIC
    else {
        diff = attr1.nVal - attr2.nVal;
    }

    // Determine the result based on the difference
    if (diff > 0) {
        return 1;
    } else if (diff < 0) {
        return -1;
    } else {
        return 0;
    }
    
    
}



int BlockBuffer::setHeader(struct HeadInfo *head)
{
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS)
        return ret;

    HeadInfo *header = (HeadInfo *)bufferPtr;

    header->numSlots = head->numSlots;
    header->numEntries = head->numEntries;
    header->numAttrs = head->numAttrs;
    header->lblock = head->lblock;
    header->rblock = head->rblock;
    header->pblock = head->pblock;

    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::setBlockType(int blockType)
{
	unsigned char *bufferPtr;
	int ret=loadBlockAndGetBufferPtr(&bufferPtr);
	if(ret!=SUCCESS)
	return ret;
	
	*((int32_t*)bufferPtr)=blockType;
	
	StaticBuffer::blockAllocMap[this->blockNum]=blockType;
	
	
	 return StaticBuffer::setDirtyBit(this->blockNum);
}


int BlockBuffer::getFreeBlock(int BlockType)
{
	int freeBlock=-1;
	for(int i=0;i<DISK_BLOCKS;i++)
	{
		if(StaticBuffer::blockAllocMap[i]==UNUSED_BLK)
		{
			freeBlock=i;
			break;
		}
	}
	
	if(freeBlock==-1)
	{
		return E_DISKFULL;
	}
	
	this->blockNum=freeBlock;
	
	int freeBuffer=StaticBuffer::getFreeBuffer(freeBlock);
	HeadInfo header;
    	header.pblock = -1;
    	header.lblock = -1;
    	header.rblock = -1;
    	header.numEntries = 0;
    	header.numAttrs = 0;
    	header.numSlots = 0;
    	
    	
    	this->setHeader(&header);
    	this->setBlockType(BlockType);

    	return freeBlock;
    	
}

int RecBuffer::setSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;
  /* get the starting address of the buffer containing the block using
     loadBlockAndGetBufferPtr(&bufferPtr). */
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }
  // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
  // return the value returned by the call.

  // get the header of the block using the getHeader() function
  HeadInfo header;
  getHeader(&header);
  int numSlots = header.numSlots; /* the number of slots in the block */
  ;
  memcpy(bufferPtr + HEADER_SIZE, slotMap, numSlots);
  // the slotmap starts at bufferPtr + HEADER_SIZE. Copy the contents of the
  // argument `slotMap` to the buffinter replacing the existing slotmap.
  // Note that size of slotmap is `numSlots`
  ret = StaticBuffer::setDirtyBit(this->blockNum);
  // update dirty bit using StaticBuffer::setDirtyBit
  // if setDirtyBit failed, return the value returned by the call
  return SUCCESS;

  // return SUCCESS
}

int BlockBuffer::getBlockNum() {
  return this->blockNum;

  // return corresponding block number.
}


void BlockBuffer::releaseBlock(){

  // if blockNum is INVALID_BLOCKNUM (-1), or it is invalidated already, do nothing
  if ( this->blockNum == INVALID_BLOCKNUM || StaticBuffer::blockAllocMap[this->blockNum] == UNUSED_BLK) return;

  // Try to get the buffer number if block is loaded in 
  int buffNum = StaticBuffer::getBufferNum(this->blockNum);
  if ( buffNum != E_BLOCKNOTINBUFFER ){
    StaticBuffer::metainfo[buffNum].free = true;
  }

  // Indicate block is free in block allocation map 
  StaticBuffer::blockAllocMap[this->blockNum] = UNUSED_BLK;
  this->blockNum = -1;
}
  
  
  
  
