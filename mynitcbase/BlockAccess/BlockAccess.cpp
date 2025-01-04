#include "BlockAccess.h"
#include<iostream>
#include <cstring>


RecId BlockAccess::linearSearch(int relId,char attrName[ATTR_SIZE],
                                union Attribute attrVal, int op) {
  // get the previous search index of the relation relId from the relation cache
  // (use RelCacheTable::getSearchIndex() function)
  RecId prevRecId;
  RelCacheTable::getSearchIndex(relId, &prevRecId);
  // let block and slot denote the record id of the record being currently
  // checked
  int block=-1, slot = -1;
  // if the current search index record is invalid(i.e. both block and slot =
  // -1)
  if (prevRecId.block == -1 && prevRecId.slot == -1) {
    // (no hits from previous search; search should start from the
    // first record itself)

    // get the first record block of the relation from the relation cache
    // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
    RelCatEntry RelCatBuf;
    RelCacheTable::getRelCatEntry(relId, &RelCatBuf);
    // block = first record block of the relation
    block = RelCatBuf.firstBlk;
    slot = 0;
    // slot = 0
  } else {
    // (there is a hit from previous search; search should start from
    // the record next to the search index record)

    // block = search index's block
    // slot = search index's slot + 1
    block = prevRecId.block;
    slot = prevRecId.slot + 1;
  }

  /* The following code searches for the next record in the relation
     that satisfies the given condition
     We start from the record id (block, slot) and iterate over the remaining
     records of the relation
  */
 RelCatEntry relCatBuffer;
	RelCacheTable::getRelCatEntry(relId, &relCatBuffer);
  while (block != -1) {
    /* create a RecBuffer object for block (use RecBuffer Constructor for
       existing block) */
    RecBuffer Buffer(block);

    HeadInfo header;
    Attribute CatRecord[RELCAT_NO_ATTRS];

    // get the record with id (block, slot) using RecBuffer::getRecord()
    Buffer.getRecord(CatRecord, slot);
    // get header of the block using RecBuffer::getHeader() function
    Buffer.getHeader(&header);
    // get slot map of the block using RecBuffer::getSlotMap() function
    unsigned char *slotMap =(unsigned char *)malloc(sizeof(unsigned char) * header.numSlots);
    Buffer.getSlotMap(slotMap);

    // If slot >= the number of slots per block(i.e. no more slots in this
    // block)
    // if (slot >= header.numSlots) {
    //   // update block = right block of block
    //   block = header.rblock;
    //   slot = 0;
    //   // update slot = 0
    //   continue; // continue to the beginning of this while loop
    // }

    if (slot >= relCatBuffer.numSlotsPerBlk)
		{
			block =header.rblock, slot = 0;
			continue; // continue to the beginning of this while loop
		}

    // if slot is free skip the loop
    // (i.e. check if slot'th entry in slot map of block contains
    // SLOT_UNOCCUPIED)
    if (slotMap[slot] == SLOT_UNOCCUPIED) {
      slot++;
      continue;
      // increment slot and continue to the next record slot
    }

    // compare record's attribute value to the the given attrVal as below:
    /*
        firstly get the attribute offset for the attrName attribute
        from the attribute cache entry of the relation using
        AttrCacheTable::getAttrCatEntry()
    */
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);


    /* use the attribute offset to get the value of the attribute from
       current record */
    Attribute *record =(Attribute *)malloc(sizeof(Attribute) * header.numAttrs);
    Buffer.getRecord(record, slot);
    int attrOffset=attrCatBuf.offset;

    int cmpVal = compareAttrs(record[attrOffset], attrVal,attrCatBuf.attrType); // will store the difference between the attributes
    // set cmpVal using compareAttrs()
    /* Next task is to check whether this record satisfies the given condition.
       It is determined based on the output of previous comparison and
       the op value received.
       The following code sets the cond variable if the condition is satisfied.
    */
    if ((op == NE && cmpVal != 0) || // if op is "not equal to"
        (op == LT && cmpVal < 0) ||  // if op is "less than"
        (op == LE && cmpVal <= 0) || // if op is "less than or equal to"
        (op == EQ && cmpVal == 0) || // if op is "equal to"
        (op == GT && cmpVal > 0) ||  // if op is "greater than"
        (op == GE && cmpVal >= 0)    // if op is "greater than or equal to"
    ) {
      /*
      set the search index in the relation cache as
      the record id of the record that satisfies the given condition
      (use RelCacheTable::setSearchIndex function)
      */
      RecId newIndex;
      newIndex.block = block;
      newIndex.slot = slot;
      RelCacheTable::setSearchIndex(relId, &newIndex);
      return RecId{block, slot};
    }
    slot++;
  }

  // no record in the relation with Id relid satisfies the given condition
  return RecId{-1, -1};
}



int BlockAccess::renameRelation(char oldName[ATTR_SIZE],char newName[ATTR_SIZE]) {
  /* reset the searchIndex of the relation catalog using
     RelCacheTable::resetSearchIndex() */
  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  Attribute newRelationName; // set newRelationName with newName
  strcpy(newRelationName.sVal, newName);
  char def_RELCAT_ATTR_RELNAME[16] = "RelName";
  // search the relation catalog for an entry with "RelName" = newRelationName
  RecId relcatRecId = BlockAccess::linearSearch(
      RELCAT_RELID, def_RELCAT_ATTR_RELNAME, newRelationName, EQ);

  // If relation with name newName already exists (result of linearSearch
  //                                               is not {-1, -1})
  //    return E_RELEXIST;

  if (relcatRecId.block != -1 and relcatRecId.slot != -1) {

    return E_RELEXIST;
  }

  /* reset the searchIndex of the relation catalog using
     RelCacheTable::resetSearchIndex() */
  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  Attribute oldRelationName; // set oldRelationName with oldName
  strcpy(oldRelationName.sVal, oldName);

  relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, def_RELCAT_ATTR_RELNAME,
                                          oldRelationName, EQ);

  if (relcatRecId.block == -1 and relcatRecId.slot == -1) {

    return E_RELNOTEXIST;
  }

  // search the relation catalog for an entry with "RelName" = oldRelationName

  // If relation with name oldName does not exist (result of linearSearch is
  // {-1, -1})
  //    return E_RELNOTEXIST;

  /* get the relation catalog record of the relation to rename using a RecBuffer
     on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
  */
  RecBuffer Buffer(relcatRecId.block);
  Attribute CatRecord[RELCAT_NO_ATTRS];
  Buffer.getRecord(CatRecord, relcatRecId.slot);
  strcpy(CatRecord[RELCAT_REL_NAME_INDEX].sVal, newName);
  /* update the relation name attribute in the record with newName.
     (use RELCAT_REL_NAME_INDEX) */
  // set back the record value using RecBuffer.setRecord
  Buffer.setRecord(CatRecord, relcatRecId.slot);

  /*TODO::update all the attribute catalog entries in the attribute catalog
  corresponding to the relation with relation name oldName to the relation name
  newName
  */

  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  /* reset the searchIndex of the attribute catalog using
     RelCacheTable::resetSearchIndex() */
     char def_ATTRCAT_ATTR_RELNAME[16] = "RelName";
  for (int i = 0; i < CatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal; i++) {
    relcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, def_ATTRCAT_ATTR_RELNAME,
                                            oldRelationName, EQ);
    RecBuffer attrCatBlock(relcatRecId.block);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    attrCatBlock.getRecord(attrCatRecord, relcatRecId.slot);
    strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, newName);
    attrCatBlock.setRecord(attrCatRecord, relcatRecId.slot);
  }
  // for i = 0 to numberOfAttributes :
  //    linearSearch on the attribute catalog for relName = oldRelationName
  //    get the record using RecBuffer.getRecord
  //
  //    update the relName field in the record to newName
  //    set back the record using RecBuffer.setRecord

  return SUCCESS;
}



int BlockAccess::renameAttribute(char relName[ATTR_SIZE],
                                 char oldName[ATTR_SIZE],
                                 char newName[ATTR_SIZE]) {

  /* reset the searchIndex of the relation catalog using
     RelCacheTable::resetSearchIndex() */
  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  Attribute relNameAttr; // set relNameAttr to relName
  strcpy(relNameAttr.sVal, relName);
  // Search for the relation with name relName in relation catalog using
  // linearSearch()
  char def_RELCAT_ATTR_RELNAME[16] = "RelName";
  RecId relcatRecId = BlockAccess::linearSearch(
      RELCAT_RELID, def_RELCAT_ATTR_RELNAME, relNameAttr, EQ);
  // If relation with name relName does not exist (search returns {-1,-1})
  //    return E_RELNOTEXIST;
  if (relcatRecId.block == -1 and relcatRecId.slot == -1)
    return E_RELNOTEXIST;

  /* reset the searchIndex of the attribute catalog usin
     RelCacheTable::resetSearchIndex() */
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

  /* declare variable attrToRenameRecId used to store the attr-cat recId
  of the attribute to rename */
  RecId attrToRenameRecId{-1,-1};
  Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

  /* iterate over all Attribute Catalog Entry record corresponding to the
     relation to find the required attribute */
     char def_ATTRCAT_ATTR_RELNAME[16] = "RelName";
  while (true) {
    RecId searchIndex = BlockAccess::linearSearch(
        ATTRCAT_RELID, def_ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
    // linear search on the attribute catalog for RelName = relNameAttr
    if (searchIndex.block == -1 and searchIndex.slot == -1)
      break;
    // if there are no more attributes left to check (linearSearch returned
    // {-1,-1})
    //     break;
    RecBuffer attrCatBlock(searchIndex.block);
    attrCatBlock.getRecord(attrCatEntryRecord, searchIndex.slot);

    /* Get the record from the attribute catalog using RecBuffer.getRecord
      into attrCatEntryRecord */
      //todo::be careful
    if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0) {
      attrToRenameRecId = searchIndex;
      break;
    }

    if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0){
			return E_ATTREXIST;
		}
  }

  // if attrToRenameRecId == {-1, -1}
  //     return E_ATTRNOTEXIST;
  if(attrToRenameRecId.slot==-1 and attrToRenameRecId.block==-1){
    return E_ATTRNOTEXIST;
  }
  RecBuffer attrCatBlock(attrToRenameRecId.block);
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  attrCatBlock.getRecord(attrCatRecord,attrToRenameRecId.slot);
  strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName);
  attrCatBlock.setRecord(attrCatRecord,attrToRenameRecId.slot);
  
  return SUCCESS;
}





int BlockAccess::insert(int relId, Attribute *record) {
  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(relId, &relCatEntry);
  // get the relation catalog entry from relation cache
  // ( use RelCacheTable::getRelCatEntry() of Cache Layer)

  int blockNum = relCatEntry.firstBlk; /* first record block of the relation
                                          (from the rel-cat entry)*/
  ;

  // rec_id will be used to store where the new record will be inserted
  RecId rec_id = {-1, -1};

  int numOfSlots =
      relCatEntry.numSlotsPerBlk; /* number of slots per record block */
  ;
  int numOfAttributes =
      relCatEntry.numAttrs; /* number of attributes of the relation */
  ;

  int prevBlockNum =
      -1; /* block number of the last element in the linked list = -1 */
  ;

  /*
      Traversing the linked list of existing record blocks of the relation
      until a free slot is found OR
      until the end of the list is reached
  */

  while (blockNum != -1) {
    RecBuffer recBuffer(blockNum);
    HeadInfo header;
    recBuffer.getHeader(&header);
    // create a RecBuffer object for blockNum (using appropriate constructor!)

    // get header of block(blockNum) using RecBuffer::getHeader() function
    unsigned char *slotMap =
        (unsigned char *)malloc(sizeof(unsigned char) * header.numSlots);
    recBuffer.getSlotMap(slotMap);
    // get slot map of block(blockNum) using RecBuffer::getSlotMap() function

    // search for free slot in the block 'blockNum' and store it's rec-id in
    // rec_id (Free slot can be found by iterating over the slot map of the
    // block)
    /* slot map stores SLOT_UNOCCUPIED if slot is free and
       SLOT_OCCUPIED if slot is occupied) */
    for (int slot = 0; slot < header.numSlots; slot++) {
      if (slotMap[slot] == SLOT_UNOCCUPIED) {
        rec_id.block = blockNum;
        rec_id.slot = slot;
        break;
      }
    }
    if (rec_id.slot != -1 and rec_id.block != -1) {
      break;
    }

    prevBlockNum = blockNum;
    blockNum = header.rblock;

    
  }

  //  if no free slot is found in existing record blocks (rec_id = {-1, -1})
  if (rec_id.block == -1 and rec_id.slot == -1) {
    // if relation is RELCAT, do not allocate any more blocks
    //     return E_MAXRELATIONS;
    if (relId == RELCAT_RELID) {
      return E_MAXRELATIONS;
    }

    // Otherwise,
    // get a new record block (using the appropriate RecBuffer constructor!)
    // get the block number of the newly allocated block
    // (use BlockBuffer::getBlockNum() function)
    RecBuffer blockBuffer;
    blockNum = blockBuffer.getBlockNum();
    // let ret be the return value of getBlockNum() function call
    if (blockNum == E_DISKFULL) {
      return E_DISKFULL;
    }

    // Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0
    rec_id.block = blockNum;
    rec_id.slot = 0;

    /*
        set the header of the new record block such that it links with
        existing record blocks of the relation
        set the block's header as follows:
        blockType: REC, pblock: -1
        lblock
              = -1 (if linked list of existing record blocks was empty
                     i.e this is the first insertion into the relation)
              = prevBlockNum (otherwise),
        rblock: -1, numEntries: 0,
        numSlots: numOfSlots, numAttrs: numOfAttributes
        (use BlockBuffer::setHeader() function)
    */
    HeadInfo blockheader;
    blockheader.pblock = blockheader.rblock = -1;
    blockheader.blockType = REC;
    blockheader.lblock = -1;
    // if (relCatEntry.numRecs == 0) {
    //    blockheader.lblock = -1;
    // } else {
    //   blockheader.lblock = prevBlockNum;
    // }
    
    blockheader.numAttrs = relCatEntry.numAttrs;
    blockheader.numEntries = 0;
    blockheader.numSlots = relCatEntry.numSlotsPerBlk;
    blockBuffer.setHeader(&blockheader);
    

    /*
        set block's slot map with all slots marked as free
        (i.e. store SLOT_UNOCCUPIED for all the entries)
        (use RecBuffer::setSlotMap() function)
    */
    unsigned char *slotMap = (unsigned char *)malloc(sizeof(unsigned char) * relCatEntry.numSlotsPerBlk);
    for (int slot = 0; slot < relCatEntry.numSlotsPerBlk; slot++) {
      slotMap[slot] = SLOT_UNOCCUPIED;
    }
    blockBuffer.setSlotMap(slotMap);

    // if prevBlockNum != -1
    if (prevBlockNum != -1) {
      // create a RecBuffer object for prevBlockNum
      // get the header of the block prevBlockNum and
      // update the rblock field of the header to the new block
      // number i.e. rec_id.block
      // (use BlockBuffer::setHeader() function)
      RecBuffer prevBuffer(prevBlockNum);
      HeadInfo prevHeader;
      prevBuffer.getHeader(&prevHeader);
      prevHeader.rblock = blockNum;
      prevBuffer.setHeader(&prevHeader);
    } else // else
    {
      // update first block field in the relation catalog entry to the
      // new block (using RelCacheTable::setRelCatEntry() function)
      relCatEntry.firstBlk = rec_id.block;
      RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }
    relCatEntry.lastBlk = rec_id.block;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    // update last block field in the relation catalog entry to the
    // new block (using RelCacheTable::setRelCatEntry() function)
  }

  // create a RecBuffer object for rec_id.block
  // insert the record into rec_id'th slot using RecBuffer.setRecord())
  RecBuffer blockBuffer(rec_id.block);
 int ret= blockBuffer.setRecord(record,rec_id.slot);
 if(ret!=SUCCESS){
  printf("Record not saved successfully.\n");
        exit(1);
 }
  /* update the slot map of the block by marking entry of the slot to
     which record was inserted as occupied) */
  // (ie store SLOT_OCCUPIED in free_slot'th entry of slot map)
  // (use RecBuffer::getSlotMap() and RecBuffer::setSlotMap() functions)
  unsigned char *slotMap=(unsigned char *)malloc(sizeof(unsigned char )*relCatEntry.numSlotsPerBlk);
  blockBuffer.getSlotMap(slotMap);
  slotMap[rec_id.slot]=SLOT_OCCUPIED;
  blockBuffer.setSlotMap(slotMap);
    
  // increment the numEntries field in the header of the block to
  // which record was inserted
  // (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)
  HeadInfo header;
  blockBuffer.getHeader(&header);
  header.numEntries=header.numEntries+1;
  blockBuffer.setHeader(&header);
  // Increment the number of records field in the relation cache entry for
  // the relation. (use RelCacheTable::setRelCatEntry function)

  relCatEntry.numRecs++;
  
  RelCacheTable::setRelCatEntry(relId,&relCatEntry);

  return SUCCESS;
}



int BlockAccess::search(  int relId, 
                          Attribute *record, 
                          char attrName[ATTR_SIZE], 
                          Attribute attrVal, 
                          int op ) {

  // Try Linear Searching
  RecId recId;
  recId = linearSearch(relId, attrName, attrVal, op);
  
  if ( recId.slot == -1 && recId.block == -1 )
    return E_NOTFOUND;

  //Fetch the required record
  RecBuffer block(recId.block);
  block.getRecord(record, recId.slot);

  return SUCCESS;
}


int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {

  // if the relation to delete is either Relation Catalog or Attribute Catalog
  if ( strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0 )
    return E_NOTPERMITTED;

  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  Attribute relNameAttr; 
  strcpy(relNameAttr.sVal, relName);

  char relnameAttrConst[] = RELCAT_ATTR_RELNAME;

  RecId rec_id = linearSearch(RELCAT_RELID, relnameAttrConst, relNameAttr, EQ);

  if ( rec_id.slot == -1 && rec_id.block == -1 )
    return E_RELNOTEXIST;

  Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
  /* store the relation catalog record corresponding to the relation in
      relCatEntryRecord using RecBuffer.getRecord */
  RecBuffer relCatBlock(rec_id.block);
  relCatBlock.getRecord(relCatEntryRecord, rec_id.slot);

  int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
  int numAttrs = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

  // Delete all the record blocks of the relation
  while ( firstBlock != -1 ){
    BlockBuffer tempBlock(firstBlock);

    HeadInfo header;
    tempBlock.getHeader(&header);
    firstBlock = header.rblock;

    tempBlock.releaseBlock();
  }


  /*
    Deleting attribute catalog entries corresponding the relation and index
    blocks corresponding to the relation with relName on its attributes
  */

  // reset the searchIndex of the attribute catalog
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

  int numberOfAttributesDeleted = 0;

  while(true) {
    
    char relnameAttrInAttrCat[] = ATTRCAT_ATTR_RELNAME;

    RecId attrCatRecId;
    attrCatRecId = linearSearch(ATTRCAT_RELID, relnameAttrInAttrCat, relNameAttr, EQ);

    if ( attrCatRecId.block == -1 && attrCatRecId.slot == -1) break;

    numberOfAttributesDeleted++;

    RecBuffer attrCatBlock(attrCatRecId.block);

    HeadInfo header;
    attrCatBlock.getHeader(&header);

    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    attrCatBlock.getRecord(attrCatRecord, attrCatRecId.slot);


    // declare variable rootBlock which will be used to store the root
    // block field from the attribute catalog record.
    int rootBlock = attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
    // (This will be used later to delete any indexes if it exists)

    // Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
    // Hint: use RecBuffer.getSlotMap and RecBuffer.setSlotMap
    unsigned char slotMap[header.numSlots];
    attrCatBlock.getSlotMap(slotMap);
    slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
    attrCatBlock.setSlotMap(slotMap);

    /* Decrement the numEntries in the header of the block corresponding to
        the attribute catalog entry and then set back the header */
    header.numEntries--;
    attrCatBlock.setHeader(&header);

    // If number of entries become 0, releaseBlock is called after fixing the linked list.
    if (header.numEntries == 0) {
      /* Standard Linked List Delete for a Block
          Get the header of the left block and set it's rblock to this
          block's rblock
      */
      int lBlockNum = header.lblock;

      // lBlockNum will never because -1 because the case that number of records in attribute cat 
      // becomes zero can never happen because RELCAT and ATTRCAT will have attributes (12)
      if ( lBlockNum != -1 ){
        RecBuffer leftBlock(lBlockNum);
        
        HeadInfo leftBlockHeader;
        leftBlock.getHeader(&leftBlockHeader);
        leftBlockHeader.rblock = header.rblock;///////////////////////////////////////////////////////
        leftBlock.setHeader(&leftBlockHeader);
      }
        

      // Setting the right block to point to the left if right is not -1
      if ( header.rblock != -1 ) {
        int rBlockNum = header.rblock;
        RecBuffer rightBlock(rBlockNum);
        
        HeadInfo rightBlockHeader;
        rightBlock.getHeader(&rightBlockHeader);
        rightBlockHeader.lblock = lBlockNum;////////////////////////////////////////////////////////
        rightBlock.setHeader(&rightBlockHeader);

      } 
      else {
        // the block being released is the "Last Block" of the relation.
        RelCatEntry attrCatInRelCatEntry;
        RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatInRelCatEntry);
        attrCatInRelCatEntry.lastBlk = lBlockNum;
        RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatInRelCatEntry);
      }

      // (Since the attribute catalog will never be empty(why?), we do not
      //  need to handle the case of the linked list becoming empty - i.e
      //  every block of the attribute catalog gets released.)

      // call releaseBlock()
      attrCatBlock.releaseBlock();
    }

    // (the following part is only relevant once indexing has been implemented)
    // if index exists for the attribute (rootBlock != -1), call bplus destroy
    // if (rootBlock != -1) {
        // delete the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
    // }
  }

  /*** Delete the entry corresponding to the relation from relation catalog ***/
  // Fetch the header of Relcat block and decrement numEntries and set it back
  HeadInfo header;
  relCatBlock.getHeader(&header);
  header.numEntries--;
  relCatBlock.setHeader(&header);

  unsigned char slotMap[header.numSlots];
  relCatBlock.getSlotMap(slotMap);
  slotMap[rec_id.slot] = SLOT_UNOCCUPIED;
  relCatBlock.setSlotMap(slotMap);


  /*** Updating the Relation Cache Table ***/
  /** Update relation catalog record entry (number of records in relation
      catalog is decreased by 1) **/
  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
  relCatEntry.numRecs--;
  RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntry);

  /** Update attribute catalog entry (number of records in attribute catalog
      is decreased by numberOfAttributesDeleted) **/
  // i.e., #Records = #Records - numberOfAttributesDeleted

  RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntry);
  relCatEntry.numRecs -= numberOfAttributesDeleted;
  RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntry);

  return SUCCESS;
}


int BlockAccess::project(int relId, Attribute *record) {
  RecId prevRecId;
  int ret = RelCacheTable::getSearchIndex(relId, &prevRecId);
  int block = prevRecId.block, slot = prevRecId.slot;

  if (prevRecId.block == -1 or prevRecId.slot == -1) {
    RelCatEntry relCatEntry;
    ret = RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    block = relCatEntry.firstBlk;
    slot = 0;
  } else {
    block = prevRecId.block;
    slot = prevRecId.slot + 1;
  }

  while (block != -1) {
    RecBuffer recordBlock(block);
    HeadInfo header;
    recordBlock.getHeader(&header);
    unsigned char slotMap[header.numSlots];
    recordBlock.getSlotMap(slotMap);

    if (slot >= header.numSlots) {
      block = header.rblock;
      slot = 0;

    } else if (slotMap[slot] == SLOT_UNOCCUPIED) {
      slot++;
    } else {
      break;
    }
  }
//all records exhausted
  if(block==-1){
    return E_NOTFOUND;
  }
  RecId nextRecId;
  nextRecId.block=block;
  nextRecId.slot=slot;

  RelCacheTable::setSearchIndex(relId,&nextRecId);
  RecBuffer recordBlock(block);
  recordBlock.getRecord(record,slot);
  return SUCCESS;
  }


