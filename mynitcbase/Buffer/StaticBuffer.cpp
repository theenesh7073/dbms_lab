#include "StaticBuffer.h"
#include <cstring>
unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

/*StaticBuffer::StaticBuffer()
{
	for(int i=0;i<BUFFER_CAPACITY;i++)
	{
		metainfo[i].free=true;
	}
}

StaticBuffer::~StaticBuffer(){}

int StaticBuffer::getFreeBuffer(int blockNum)
{
	if(blockNum<0 || blockNum>=DISK_BLOCKS) {return E_OUTOFBOUND;}
	int allocatedBuffer=-1;
	
	for(int i=0;i<BUFFER_CAPACITY;i++)
	{
		if(metainfo[i].free) { allocatedBuffer=i; break;}
	}
	
	metainfo[allocatedBuffer].free=false;
	metainfo[allocatedBuffer].blockNum=blockNum;
	return allocatedBuffer;
}*/


StaticBuffer::StaticBuffer() {
	unsigned char block[BLOCK_SIZE];
	for(int i=0;i<=3;i++)
	{
		Disk::readBlock(block,i);
		memcpy(blockAllocMap+i*BLOCK_SIZE,block,BLOCK_SIZE);
	}
  // initialise all blocks as free
  for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
    metainfo[bufferIndex].free = true;
    metainfo[bufferIndex].dirty=false;
    metainfo[bufferIndex].blockNum=-1;
    metainfo[bufferIndex].timeStamp=-1;
  }
}


StaticBuffer::~StaticBuffer()
{
    // copy blockAllocMap blocks from buffer to disk(using writeblock() of disk)
    // blocks 0 to 3
    for (int blockNum = 0; blockNum < 4; blockNum++)
    {
        Disk::writeBlock(blockAllocMap + blockNum * BLOCK_SIZE, blockNum);
    }
    /*iterate through all the buffer blocks,
      write back blocks with metainfo as free=false,dirty=true
      using Disk::writeBlock()
      */
    for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++)
    {
        if (!metainfo[bufferIndex].free && metainfo[bufferIndex].dirty)
        {
            Disk::writeBlock(blocks[bufferIndex], metainfo[bufferIndex].blockNum);
        }
    }
}
int StaticBuffer::getFreeBuffer(int blockNum) {
  // Assigns a buffer to the block and returns the buffer number. If no free
  // buffer block is found, the least recently used (LRU) buffer block is
  // replaced.

  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  int allocatedBuffer=-1;
  // iterate through all the blocks in the StaticBuffer
  // find the first free block in the buffer (check metainfo)
  // assign allocatedBuffer = index of the free block
  int timeStamp=0,maxindex=0;

  for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
     
     if(metainfo[bufferIndex].timeStamp>timeStamp){
      timeStamp=metainfo[bufferIndex].timeStamp;
      maxindex=bufferIndex;
     }
    if(metainfo[bufferIndex].free) {
      allocatedBuffer = bufferIndex;
      break;
    }
  }

  if(allocatedBuffer==-1){
    if(metainfo[maxindex].dirty==true){
      Disk::writeBlock(blocks[maxindex],metainfo[maxindex].blockNum);
      allocatedBuffer=maxindex;
    }
  }


  metainfo[allocatedBuffer].free = false;
  metainfo[allocatedBuffer].blockNum = blockNum;
  metainfo[allocatedBuffer].dirty=false;
  metainfo[allocatedBuffer].timeStamp=0;

  return allocatedBuffer;
}

int StaticBuffer::getBufferNum(int blockNum) {
  if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }

  for (int i = 0; i < BUFFER_CAPACITY; ++i) {
    if (metainfo[i].blockNum == blockNum) {
      return i;
    }
  }

  return E_BLOCKNOTINBUFFER;
}


int StaticBuffer::setDirtyBit(int blockNum){
    // find the buffer index corresponding to the block using getBufferNum().
    int bufferIndex=getBufferNum(blockNum);

    // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
    //     return E_BLOCKNOTINBUFFER
    if(bufferIndex==E_BLOCKNOTINBUFFER){
      return E_BLOCKNOTINBUFFER;
    }
    if(bufferIndex==E_OUTOFBOUND){
      return E_OUTOFBOUND;
    }
    else
    {
    
      metainfo[bufferIndex].dirty=true;
    }
    return SUCCESS;
}
