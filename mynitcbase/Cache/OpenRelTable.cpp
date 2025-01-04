#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>
#include <stdio.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
using namespace std;


AttrCacheEntry *createAttrCacheEntryList(int size) {
  AttrCacheEntry *head = nullptr, *curr = nullptr;
  head = curr = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
  size--;
  while (size--) {
    curr->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    curr = curr->next;
  }
  curr->next = nullptr;
  return head;
}

void freeLinkedList(AttrCacheEntry *head)
{
    for (AttrCacheEntry *it = head, *next; it != nullptr; it = next)
    {
        next = it->next;
        free(it);
    }
}




OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];


OpenRelTable::OpenRelTable() {

    // Initialize all values in relCache and attrCache to be nullptr
    for (int i = 0; i < MAX_OPEN; ++i) {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
        // Initialize all entries in tableMetaInfo to be free
        tableMetaInfo[i].free = true;
    }

    /************ Setting up Relation Cache entries ************/

    // Populate relation cache with entries for the relation catalog and attribute catalog
    RecBuffer relCatBlock(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    RelCacheEntry* relCacheEntry = nullptr;

    // Setting up RELCAT (rel-id = 0)
    relCatBlock.getRecord(relCatRecord, 0);
    relCacheEntry = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry->relCatEntry);
    relCacheEntry->recId.block = RELCAT_BLOCK;
    relCacheEntry->recId.slot = 0;
    RelCacheTable::relCache[0] = relCacheEntry;

    // Update tableMetaInfo for RELCAT
    tableMetaInfo[RELCAT_RELID].free = false;
    strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);

    // Setting up ATTRCAT (rel-id = 1)
    relCatBlock.getRecord(relCatRecord, 1);
    relCacheEntry = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry->relCatEntry);
    relCacheEntry->recId.block = RELCAT_BLOCK;
    relCacheEntry->recId.slot = 1;
    RelCacheTable::relCache[1] = relCacheEntry;

    // Update tableMetaInfo for ATTRCAT
    tableMetaInfo[ATTRCAT_RELID].free = false;
    strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);

    // Setting up Students relation (rel-id = 2)
    relCatBlock.getRecord(relCatRecord, 2);
    relCacheEntry = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry->relCatEntry);
    relCacheEntry->recId.block = RELCAT_BLOCK;
    relCacheEntry->recId.slot = 2;
    RelCacheTable::relCache[2] = relCacheEntry;

    /************ Setting up Attribute Cache entries ************/

    RecBuffer attrCatBlock(ATTRCAT_BLOCK);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    AttrCacheEntry* head = nullptr;
    AttrCacheEntry* tail = nullptr;

    // Setting up RELCAT Attributes (slots 0 to 5)
    for (int i = 0; i <= 5; ++i) {
        attrCatBlock.getRecord(attrCatRecord, i);
        AttrCacheEntry* newEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &newEntry->attrCatEntry);
        newEntry->recId.block = ATTRCAT_BLOCK;
        newEntry->recId.slot = i;
        newEntry->next = nullptr;

        if (head == nullptr) {
            head = newEntry;
        } else {
            tail->next = newEntry;
        }
        tail = newEntry;
    }
    AttrCacheTable::attrCache[0] = head;

    // Setting up ATTRCAT Attributes (slots 6 to 11)
    head = nullptr;
    tail = nullptr;
    for (int i = 6; i <= 11; ++i) {
        attrCatBlock.getRecord(attrCatRecord, i);
        AttrCacheEntry* newEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &newEntry->attrCatEntry);
        newEntry->recId.block = ATTRCAT_BLOCK;
        newEntry->recId.slot = i;
        newEntry->next = nullptr;

        if (head == nullptr) {
            head = newEntry;
        } else {
            tail->next = newEntry;
        }
        tail = newEntry;
    }
    AttrCacheTable::attrCache[1] = head;

    // Setting up Students Attributes (slots 12 to 15)
    head = nullptr;
    tail = nullptr;
    for (int i = 12; i <= 15; ++i) {
        attrCatBlock.getRecord(attrCatRecord, i);
        AttrCacheEntry* newEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &newEntry->attrCatEntry);
        newEntry->recId.block = ATTRCAT_BLOCK;
        newEntry->recId.slot = i;
        newEntry->next = nullptr;

        if (head == nullptr) {
            head = newEntry;
        } else {
            tail->next = newEntry;
        }
        tail = newEntry;
    }
    AttrCacheTable::attrCache[2] = head;
}











/*int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
    // If relName is "RELCAT", return the hardcoded ID for RELCAT
    if (strcmp(relName, RELCAT_RELNAME) == 0) {
        return RELCAT_RELID;
    }

    // If relName is "ATTRCAT", return the hardcoded ID for ATTRCAT
    if (strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return ATTRCAT_RELID;
    }

    // If relName is "Students", return the hardcoded ID for Students
    if (strcmp(relName, "Students") == 0) {
        return 2; // The relId for Students is hardcoded as 2 in this example
    }

    // If the relation name doesn't match any known catalog names, return an error
    return E_RELNOTOPEN;
}*/


int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
  for (int i = 0; i < MAX_OPEN; i++) {
    if (strcmp(relName, tableMetaInfo[i].relName) == 0 and tableMetaInfo[i].free == false) {
      return i;
    }
  }
  return E_RELNOTOPEN;
}



OpenRelTable::~OpenRelTable() {

  // close all open relations from rel-id = 2 onwards.
  for (int i = 2; i < MAX_OPEN; ++i) {
    if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i);
    }
  }

  /**** Closing the catalog relations in the relation cache ****/

  //releasing the relation cache entry of the attribute catalog
  if ( RelCacheTable::relCache[ATTRCAT_RELID]->dirty == true ) {

    Attribute attrCatInRelCatRecord[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry,
                                        attrCatInRelCatRecord);

    // declaring an object of RecBuffer class to write back to the buffer
    RecId recId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;

    // Writing the record back to the block
    RecBuffer relCatBlock(recId.block);
    relCatBlock.setRecord(attrCatInRelCatRecord, recId.slot);
  }
  // free the memory dynamically allocated to this RelCacheEntry
  free(RelCacheTable::relCache[ATTRCAT_RELID]);

  //releasing the relation cache entry of the relation catalog
  if( RelCacheTable::relCache[RELCAT_RELID]->dirty == true ) {

    Attribute relCatInRelCatRecord[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[RELCAT_RELID]->relCatEntry,
                                        relCatInRelCatRecord);

    RecId recId = RelCacheTable::relCache[RELCAT_RELID]->recId;
    // declaring an object of RecBuffer class to write back to the buffer
    RecBuffer relCatBlock(recId.block);

    // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    relCatBlock.setRecord(relCatInRelCatRecord, recId.slot);
  }
  // free the memory dynamically allocated for this RelCacheEntry
  free(RelCacheTable::relCache[RELCAT_RELID]);

  // free the memory allocated for the attribute cache entries of the
  // relation catalog and the attribute catalog ??
}


int OpenRelTable::getFreeOpenRelTableEntry()
{
	for(int i=0;i<MAX_OPEN;i++)
	{
		if(tableMetaInfo[i].free)
		{
			return i;
		}
	}
	return E_CACHEFULL;
}



int OpenRelTable::openRel(char relName[ATTR_SIZE])
{
  int ret = OpenRelTable::getRelId(relName);
  if (ret != E_RELNOTOPEN)
  {
    return ret;
  }

  /* find a free slot in the Open Relation Table
     using OpenRelTable::getFreeOpenRelTableEntry(). */
  ret = OpenRelTable::getFreeOpenRelTableEntry();
  if (ret == E_CACHEFULL)
  {
    return E_CACHEFULL;
  }

  // let relId be used to store the free slot.
  int relId = ret;

  /** Setting up Relation Cache entry for the relation **/

  /* search for the entry with relation name, relName, in the Relation Catalog using
      BlockAccess::linearSearch().
      Care should be taken to reset the searchIndex of the relation RELCAT_RELID
      before calling linearSearch().*/
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  // relcatRecId stores the rec-id of the relation relName in the Relation Catalog.
  Attribute check_relname;
  strcpy(check_relname.sVal, relName);
  char def_RELCAT_ATTR_RELNAME[16] = "RelName";
  RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, def_RELCAT_ATTR_RELNAME, check_relname, EQ);

  if (relcatRecId.block == -1 && relcatRecId.slot == -1)
  {
    // (the relation is not found in the Relation Catalog.)
    return E_RELNOTEXIST;
  }

  /* read the record entry corresponding to relcatRecId and create a relCacheEntry
      on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
      update the recId field of this Relation Cache entry to relcatRecId.
      use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
    NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
  */
  RecBuffer recEntry(relcatRecId.block);
  struct RelCacheEntry recCacheEntry;
  Attribute attrRecord[RELCAT_NO_ATTRS];
  recEntry.getRecord(attrRecord, relcatRecId.slot);
  RelCacheTable::recordToRelCatEntry(attrRecord, &recCacheEntry.relCatEntry);
  recCacheEntry.recId = relcatRecId;

  RelCacheTable::relCache[relId] = (struct RelCacheEntry *)malloc(sizeof(struct RelCacheEntry));
  *(RelCacheTable::relCache[relId]) = recCacheEntry;

  /** Setting up Attribute Cache entry for the relation **/
  RecBuffer attrEntry(relcatRecId.block);
  // let listHead be used to hold the head of the linked list of attrCache entries.
  AttrCacheEntry *listHead, *curr;
  listHead = curr = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
  int totalAttrs = recCacheEntry.relCatEntry.numAttrs;
  for (int i = 0; i < totalAttrs - 1; i++)
  {
    curr->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    curr = curr->next;
  }
  curr->next = nullptr;
  curr = listHead;
  /*iterate over all the entries in the Attribute Catalog corresponding to each
  attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
  care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
  corresponding to Attribute Catalog before the first call to linearSearch().*/
  char def_ATTRCAT_ATTR_RELNAME[16] = "RelName";
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  for (int i = 0; i < totalAttrs; i++)
  {
    /* let attrcatRecId store a valid record id an entry of the relation, relName,
    in the Attribute Catalog.*/
    Attribute check_relname;
    strcpy(check_relname.sVal, relName);
    RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, def_ATTRCAT_ATTR_RELNAME, check_relname, EQ);

    /* read the record entry corresponding to attrcatRecId and create an
    Attribute Cache entry on it using RecBuffer::getRecord() and
    AttrCacheTable::recordToAttrCatEntry().
    update the recId field of this Attribute Cache entry to attrcatRecId.
    add the Attribute Cache entry to the linked list of listHead .*/
    // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
    RecBuffer recbuffer(attrcatRecId.block);
    Attribute record[ATTRCAT_NO_ATTRS];
    recbuffer.getRecord(record, attrcatRecId.slot);
    AttrCacheTable::recordToAttrCatEntry(record, &curr->attrCatEntry);
    curr->recId = attrcatRecId;
    curr = curr->next;
  }

  // set the relIdth entry of the AttrCacheTable to listHead.
  AttrCacheTable::attrCache[relId] = listHead;
  /** Setting up metadata in the Open Relation Table for the relation****/

  // update the relIdth entry of the tableMetaInfo with free as false and
  // relName as the input.
  tableMetaInfo[relId].free = false;
  strcpy(tableMetaInfo[relId].relName, relName);

  return relId;
}



int OpenRelTable::closeRel(int relId) {
  if (relId == 0 or relId == 1) {
    return E_NOTPERMITTED;
  }

  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }
  if (tableMetaInfo[relId].free)
    return E_RELNOTOPEN;
  if (AttrCacheTable::attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }


	if(RelCacheTable::relCache[relId]->dirty==true)
	{
		Attribute relcatBuffer[RELCAT_NO_ATTRS];
		RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[relId]->relCatEntry), relcatBuffer);
		RecId recId = RelCacheTable::relCache[relId]->recId;
		
		RecBuffer relCatBlock(recId.block);
		
		relCatBlock.setRecord(relcatBuffer,RelCacheTable::relCache[relId]->recId.slot);
	}
  free(RelCacheTable::relCache[relId]);
  AttrCacheEntry *head = AttrCacheTable::attrCache[relId];
  AttrCacheEntry *next = head->next;
  while (next) {
    free(head);
    head = next;
    next = next->next;
  }
  free(head);

  // free the memory allocated in the relation and attribute caches which was
  // allocated in the OpenRelTable::openRel() function
  tableMetaInfo[relId].free = true;
  AttrCacheTable::attrCache[relId] = nullptr;
  RelCacheTable::relCache[relId] = nullptr;
  // update `tableMetaInfo` to set `relId` as a free slot
  // update `relCache` and `attrCache` to set the entry at `relId` to nullptr

  return SUCCESS;
}

