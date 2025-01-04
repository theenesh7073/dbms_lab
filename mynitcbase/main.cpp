#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>

int main(int argc, char *argv[]) {
    Disk disk_run;
    StaticBuffer buffer;
    OpenRelTable cache;
/*
    RelCatEntry relCatEntry;
    AttrCatEntry attrCatEntry;

    // Iterate through relation IDs for RELCAT, ATTRCAT, and Students
    for (int i = 0; i <= 2; i++) {
        // Get the relation catalog entry from the cache
        int ret = RelCacheTable::getRelCatEntry(i, &relCatEntry);
        if (ret != SUCCESS) {
            std::cerr << "Error fetching relation catalog entry for rel-id " << i << std::endl;
            continue;
        }

        // Print the relation name
        printf("Relation: %s\n", relCatEntry.relName);

        // Iterate through attributes of the relation
        for (int j = 0; j < relCatEntry.numAttrs; j++) {
            // Get the attribute catalog entry from the cache
            ret = AttrCacheTable::getAttrCatEntry(i, j, &attrCatEntry);
            if (ret != SUCCESS) {
                std::cerr << "Error fetching attribute catalog entry for rel-id " << i << ", attr-offset " << j << std::endl;
                continue;
            }

            // Determine the attribute type
            const char* attrType = attrCatEntry.attrType == NUMBER ? "NUM" : "STR";

            // Print the attribute name and type
            printf("  %s: %s\n", attrCatEntry.attrName, attrType);
        }

        printf("\n");
    }
*/
     return FrontendInterface::handleFrontend(argc, argv);
}

