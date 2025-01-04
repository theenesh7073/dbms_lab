#include "Schema.h"

#include <cmath>
#include <cstring>





int Schema::openRel(char relName[ATTR_SIZE]) {
  int ret = OpenRelTable::openRel(relName);

  if(ret >= 0){
    return SUCCESS;
  }

  
  return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE])
{
	if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
		return E_NOTPERMITTED;
		
	int relId = OpenRelTable::getRelId(relName);

	if (relId == E_RELNOTOPEN)
		return E_RELNOTOPEN;

	return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {
    // Check if oldRelName or newRelName is either "RELATIONCAT" or "ATTRIBUTECAT"
    if (strcmp(oldRelName, RELCAT_RELNAME) == 0 || strcmp(oldRelName, ATTRCAT_RELNAME) == 0 ||
        strcmp(newRelName, RELCAT_RELNAME) == 0 || strcmp(newRelName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    // Check if the relation with oldRelName is open
    if (OpenRelTable::getRelId(oldRelName) != E_RELNOTOPEN) {
        return E_RELOPEN;
    }

    // Check if the relation with newRelName already exists
    if (OpenRelTable::getRelId(newRelName) != E_RELNOTOPEN) {
        return E_RELEXIST;
    }

    // Rename the relation using BlockAccess::renameRelation
    int retVal = BlockAccess::renameRelation(oldRelName, newRelName);
    return retVal;
}
 
 
 
int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName) {
    
        if(strcmp(relName,RELCAT_RELNAME)==0 || strcmp(relName,ATTRCAT_RELNAME)==0)return E_NOTPERMITTED;

    int relId=OpenRelTable::getRelId(relName);
    if(relId!=E_RELNOTOPEN)return E_RELOPEN;
    

    // Call BlockAccess::renameAttribute with appropriate arguments.
    return BlockAccess::renameAttribute(relName,oldAttrName,newAttrName);

    // return the value returned by the above renameAttribute() call
}

int Schema::createRel(char relName[],int nAttrs, char attrs[][ATTR_SIZE],int attrtype[]){

  // declare variable relNameAsAttribute of type Attribute
  Attribute relNameAsAttribute;
  strcpy(relNameAsAttribute.sVal, relName);
  char relnameAttrRelcat[] = RELCAT_ATTR_RELNAME;
  
  // declare a variable targetRelId of type RecId
  RecId targetRecId;
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  targetRecId = BlockAccess::linearSearch(RELCAT_RELID, relnameAttrRelcat, relNameAsAttribute, EQ );

  
  // printf("Wot : %s, %s\n", relnameAttrRelcat, relNameAsAttribute.sVal);

  if ( targetRecId.slot != -1 || targetRecId.block != -1 ) return E_RELEXIST;

  for ( int i = 0 ;i < nAttrs; i++ ){
    for ( int j = 0 ;j < nAttrs; j++ ){
      if ( i!=j && strcmp(attrs[i],attrs[j]) == 0 ) return E_DUPLICATEATTR;
    }
  }

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  // fill relCatRecord fields as given below
  strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
  relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
  relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
  relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
  relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
  relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor((2016 / (16 * nAttrs + 1)));


  int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);
  if ( retVal != SUCCESS ) return retVal;

  for ( int i = 0; i < nAttrs; i++ ){
    /* declare Attribute attrCatRecord[6] to store the attribute catalog
        record corresponding to i'th attribute of the argument passed*/
    Attribute attrCatRecord[RELCAT_NO_ATTRS];
    strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
    strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[i]);
    attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[i];
    attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
    attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
    attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;

    int retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
    if ( retVal != SUCCESS ){
      Schema::deleteRel(relName);
      return E_DISKFULL;
    }
  }

  return SUCCESS;
}


int Schema::deleteRel(char *relName) {
  if ( strcmp(relName, ATTRCAT_RELNAME) == 0 || strcmp(relName, RELCAT_RELNAME) == 0 )
    return E_NOTPERMITTED;

  int relId = OpenRelTable::getRelId(relName);
  if(relId != E_RELNOTOPEN){
    return E_RELOPEN;
  }

  int ret = BlockAccess::deleteRelation(relName);

  return ret;

  /* 
    the only that should be returned from deleteRelation() is E_RELNOTEXIST.
    The deleteRelation call may return E_OUTOFBOUND from the call to
    loadBlockAndGetBufferPtr, but if your implementation so far has been
    correct, it should not reach that point. That error could only occur
    if the BlockBuffer was initialized with an invalid block number.
  */
}

