#include "Algebra.h"
#include <cstring>
#include <cstdlib>
#include <cstdio>

/* Checks if a string can be parsed as a floating point number */
bool isNumber(char *str) {
  int len;
  float ignore;
  int ret = sscanf(str, "%f %n", &ignore, &len);
  return ret == 1 && len == strlen(str);
}
/*
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE],
                    char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if (srcRelId == E_RELNOTOPEN) {
    return E_RELNOTOPEN;
  }

  AttrCatEntry attrCatEntry;
  if (AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry) == E_ATTRNOTEXIST) {
    return E_ATTRNOTEXIST;
  }

  int type = attrCatEntry.attrType;
  Attribute attrVal;
  if (type == NUMBER) {
    if (isNumber(strVal)) {
      attrVal.nVal = atof(strVal);
    } else {
      return E_ATTRTYPEMISMATCH;
    }
  } else if (type == STRING) {
    strcpy(attrVal.sVal, strVal);
  }

  RelCacheTable::resetSearchIndex(srcRelId);

  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

  // Print attribute names as table headers
  printf("|");
  for (int i = 0; i < relCatEntry.numAttrs; ++i) {
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
    printf(" %s |", attrCatEntry.attrName);
  }
  printf("\n");

  while (true) {
    RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

    if (searchRes.block != -1 && searchRes.slot != -1) {
      AttrCatEntry attrCatEntry;
      RecBuffer BlockBuffer(searchRes.block);
      HeadInfo blockHeader;
      BlockBuffer.getHeader(&blockHeader);

      Attribute recordBuffer[blockHeader.numAttrs];
      BlockBuffer.getRecord(recordBuffer, searchRes.slot);

      for (int i = 0; i < relCatEntry.numAttrs; ++i) {
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        if (attrCatEntry.attrType == NUMBER)
          printf(" %d |", (int)recordBuffer[i].nVal);
        else
          printf(" %s |", recordBuffer[i].sVal);
      }
      printf("\n");

    } else {
      break;
    }
  }

  return SUCCESS;
}
*/




int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
    
    if(strcmp(relName,RELCAT_RELNAME)==0 or strcmp(relName,ATTRCAT_RELNAME)==0){
      return E_NOTPERMITTED;
    }

    // get the relation's rel-id using OpenRelTable::getRelId() method
    int relId = OpenRelTable::getRelId(relName);
    
    if(relId==E_RELNOTOPEN){
      return E_RELNOTOPEN;
    }

    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(relId,&relCatBuf);

    // if relation is not open in open relation table, return E_RELNOTOPEN
    // (check if the value returned from getRelId function call = E_RELNOTOPEN)
    // get the relation catalog entry from relation cache
    // (use RelCacheTable::getRelCatEntry() of Cache Layer)

    /* if relCatEntry.numAttrs != numberOfAttributes in relation,
       return E_NATTRMISMATCH */
       if(relCatBuf.numAttrs!=nAttrs){
        return E_NATTRMISMATCH;
       }

    // let recordValues[numberOfAttributes] be an array of type union Attribute
    Attribute recordValues[nAttrs];

    /*
        Converting 2D char array of record values to Attribute array recordValues
     */
    // iterate through 0 to nAttrs-1: (let i be the iterator)
    for(int i=0;i<nAttrs;i++)
    {
        // get the attr-cat entry for the i'th attribute from the attr-cache
        // (use AttrCacheTable::getAttrCatEntry())
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId,i,&attrCatEntry);
        int type=attrCatEntry.attrType;
        // let type = attrCatEntry.attrType;

        if (type == NUMBER)
        {
            // if the char array record[i] can be converted to a number
            // (check this using isNumber() function)
            if(isNumber(record[i]))
            {
                /* convert the char array to numeral and store it
                   at recordValues[i].nVal using atof() */
                   recordValues[i].nVal=atof(record[i]);
            }
            else
            {
                return E_ATTRTYPEMISMATCH;
            }
        }
        else if (type == STRING)
        {
            // copy record[i] to recordValues[i].sVal
            strcpy(recordValues[i].sVal,record[i]);
        }
    }
    int retVal=BlockAccess::insert(relId,recordValues);
    // insert the record by calling BlockAccess::insert() function
    // let retVal denote the return value of insert call

    return retVal;
}


int Algebra::select(  char srcRel[ATTR_SIZE], 
                      char targetRel[ATTR_SIZE], 
                      char attr[ATTR_SIZE], 
                      int op, 
                      char strVal[ATTR_SIZE]) {

  int srcRelId = OpenRelTable::getRelId(srcRel);
  if (srcRelId == E_RELNOTOPEN) {
    return E_RELNOTOPEN;
  }

  // get the attr-cat entry for attr
  AttrCatEntry attrCatEntry;
  int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
  if ( ret != SUCCESS ) {
    return E_ATTRNOTEXIST;
  }

  /*** Convert strVal (string) to an attribute of data type NUMBER or STRING ***/
  int type = attrCatEntry.attrType;
  Attribute attrVal;
  if (type == NUMBER) {
    if (isNumber(strVal)) {       // the isNumber() function is implemented below
      attrVal.nVal = atof(strVal);
    } else {
      return E_ATTRTYPEMISMATCH;
    }
  } else if (type == STRING) {
    strcpy(attrVal.sVal, strVal);
  }


    /************************
  The following code prints the contents of a relation directly to the output
  console. Direct console output is not permitted by the actual the NITCbase
  specification and the output can only be inserted into a new relation. We will
  be modifying it in the later stages to match the specification.
  ************************/

  // printf("|");
  // for (int i = 0; i < relCatEntry.numAttrs; ++i) {
  //   AttrCatEntry attrCatEntry;
  //   AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
  //   printf(" %s |", attrCatEntry.attrName);
  // }
  // printf("\n");

  // while (true) {

  //   RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

  //   if (searchRes.block != -1 && searchRes.slot != -1) {

  //     // get the record at searchRes using BlockBuffer.getRecord
  //     RecBuffer block( searchRes.block );

  //     HeadInfo header;
  //     block.getHeader(&header);

  //     Attribute record[header.numAttrs];
  //     block.getRecord( record, searchRes.slot );
  //     // print the attribute values in the same format as above
  //     printf("|");
  //     for (int i = 0; i < relCatEntry.numAttrs; ++i)
  //     {
  //         AttrCatEntry attrCatEntry;
  //         // get attrCatEntry at offset i using AttrCacheTable::getAttrCatEntry()
  //         int response = AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
  //         if (response != SUCCESS)
  //         {
  //             printf("Invalid Attribute ID.\n");
  //             exit(1);
  //         }

  //         if (attrCatEntry.attrType == NUMBER)
  //         {
  //             printf(" %d |", (int) record[i].nVal);
  //         }
  //         else
  //         {
  //             printf(" %s |", record[i].sVal);
  //         }
  //     }
  //     printf("\n");
  //   } 
  //   else {
  //     break;
  //   }
  // }



  /*** Creating and opening the target relation ***/
  // Prepare arguments for createRel() in the following way:
  // get RelcatEntry of srcRel using RelCacheTable::getRelCatEntry()
  RelCatEntry srcRelCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);
  int src_nAttrs =  srcRelCatEntry.numAttrs;

  char attr_names[src_nAttrs][ATTR_SIZE];

  int attr_types[src_nAttrs];

  for ( int i = 0; i < src_nAttrs; i ++ ){
    AttrCatEntry srcAttrCatEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &srcAttrCatEntry);
    strcpy(attr_names[i], srcAttrCatEntry.attrName);
    attr_types[i] = srcAttrCatEntry.attrType;
  }

  ret = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
  if ( ret != SUCCESS ) return ret;

  // Open the newly created target relation
  int tarRelId = OpenRelTable::openRel(targetRel);

  /* If opening fails, delete the target relation by calling Schema::deleteRel()
      and return the error value returned from openRel() */
  if ( tarRelId < 0 ) return tarRelId;

  /*** Selecting and inserting records into the target relation ***/
  /* Before calling the search function, reset the search to start from the
      first using RelCacheTable::resetSearchIndex() */
  Attribute record[src_nAttrs];

  /*
      The BlockAccess::search() function can either do a linearSearch or
      a B+ tree search. Hence, reset the search index of the relation in the
      relation cache using RelCacheTable::resetSearchIndex().
      Also, reset the search index in the attribute cache for the select
      condition attribute with name given by the argument `attr`. Use
      AttrCacheTable::resetSearchIndex().
      Both these calls are necessary to ensure that search begins from the
      first record.
  */
  //-----------------------B+TREE-------------------------------------------------------------------
  RelCacheTable::resetSearchIndex(srcRelId);
  // AttrCacheTable::resetSearchIndex(srcRelId, attr);

  // read every record that satisfies the condition by repeatedly calling
  // BlockAccess::search() until there are no more records to be read

  while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS ) {

    int ret = BlockAccess::insert(tarRelId, record);
    if ( ret != SUCCESS ){
      Schema::closeRel(targetRel);
      Schema::deleteRel(targetRel);
      printf("Failed to insert\n");
      return ret;
    }
  }

  // Close the relation 
  Schema::closeRel(targetRel);
  return SUCCESS;
}



int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {

  int ret;

  int srcRelId = OpenRelTable::getRelId(srcRel);
  if ( srcRelId == E_RELNOTOPEN ) return E_RELNOTOPEN;

  RelCatEntry srcRelCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);

  int numAttrs = srcRelCatEntry.numAttrs;

  // attrNames and attrTypes will be used to store the attribute names
  // and types of the source relation respectively
  char attrNames[numAttrs][ATTR_SIZE];
  int attrTypes[numAttrs];

  /*iterate through every attribute of the source relation :
      - get the AttributeCat entry of the attribute with offset.
        (using AttrCacheTable::getAttrCatEntry())
      - fill the arrays `attrNames` and `attrTypes` that we declared earlier
        with the data about each attribute
  */
  for (int i = 0; i < numAttrs ; i++ ){
    AttrCatEntry srcAttrCatEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &srcAttrCatEntry);
    strcpy(attrNames[i], srcAttrCatEntry.attrName);
    attrTypes[i] = srcAttrCatEntry.attrType;
  }


  /*** Creating and opening the target relation ***/

  ret = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);
  if ( ret != SUCCESS ) return ret;

  int tarRelId = OpenRelTable::openRel(targetRel);

  if ( tarRelId < 0 ) {
    Schema::deleteRel(targetRel);
    return tarRelId;
  }

  /*** Inserting projected records into the target relation ***/

  RelCacheTable::resetSearchIndex(srcRelId);
  Attribute record[numAttrs];


  while (BlockAccess::project(srcRelId, record) == SUCCESS )
  {
    ret = BlockAccess::insert(tarRelId, record);

    if (ret != SUCCESS) {
      Schema::closeRel(targetRel);
      Schema::deleteRel(targetRel);
      return ret;
    }
  }
  Schema::closeRel(targetRel);
  return SUCCESS;
}

// Porject only specified columns into another relation
int Algebra::project( char srcRel[ATTR_SIZE], 
                      char targetRel[ATTR_SIZE], 
                      int tar_nAttrs, 
                      char tar_Attrs[][ATTR_SIZE] ) {
  
  int ret;

  int srcRelId = OpenRelTable::getRelId(srcRel);
  if ( srcRelId == E_RELNOTOPEN ) return E_RELNOTOPEN;

  RelCatEntry srcRelCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);

  int src_nAttrs = srcRelCatEntry.numAttrs;

  // where i-th entry will store the offset in a record of srcRel for the
  // i-th attribute in the target relation.
  int attr_offset[tar_nAttrs];  
  // where i-th entry will store the type of the i-th attribute in the
  // target relation.
  int attr_types[tar_nAttrs];


  /*** Checking if attributes of target are present in the source relation
       and storing its offsets and types ***/
  for (int i = 0; i < tar_nAttrs; i ++ ){
    AttrCatEntry tarAttrCatEntry;
    ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &tarAttrCatEntry);
    if ( ret != SUCCESS ) return E_ATTRNOTEXIST;
    attr_offset[i] = tarAttrCatEntry.offset;
    attr_types[i] = tarAttrCatEntry.attrType;
  }

  /*** Creating and opening the target relation ***/
  ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
  if ( ret != SUCCESS ) return ret;

  // Try to open and delete relation if open fails
  int tarRelId = OpenRelTable::openRel(targetRel);

  if ( tarRelId < 0 ) {
    Schema::deleteRel(targetRel);
    return tarRelId;
  }


  /*** Inserting projected records into the target relation ***/

  RelCacheTable::resetSearchIndex(srcRelId);
  Attribute record[src_nAttrs];

  while (BlockAccess::project(srcRelId, record) == SUCCESS ){
    // the variable `record` will contain the next record
    Attribute proj_record[tar_nAttrs];

    for (int i = 0; i < tar_nAttrs; i++){
      proj_record[i] = record[attr_offset[i]];
      // if ( attr_types[i] == NUMBER ) proj_record[i].nVal = record[attr_offset[i]].nVal;
      // else strcpy(proj_record[i].sVal, record[attr_offset[i]].sVal);
    }

    ret = BlockAccess::insert(tarRelId, proj_record);

    if (ret != SUCCESS) {
      Schema::closeRel(targetRel);
      Schema::deleteRel(targetRel);
      return ret;
    }
  }
  Schema::closeRel(targetRel);
  return SUCCESS;
}
