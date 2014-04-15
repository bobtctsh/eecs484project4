#include "catalog.h"
#include "query.h"
#include "index.h"
#include <cstring>
/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    /* Your solution goes here */
    int iattrCnt;
    AttrDesc *relattrs;
    attrCat->getRelInfo(relation, iattrCnt, relattrs);

    if(attrCnt != iattrCnt)
        ;//number of attr doesnt match

    std::cout << relation << ' ' << *(int *)(attrList[0].attrValue) << endl;
    std::cout << "GG: " << relattrs[0].relName << endl;
    std::cout << "L: " << sizeof(attrList[0].attrName) << endl;
    int totalLen = 0;
    for(int i = 0; i < iattrCnt; i++)
        totalLen += relattrs[i].attrLen;
    Record irecord;
    irecord.length = totalLen;
    irecord.data = new char[irecord.length];

    //memcpy all the attr into irecord
    for(int i = 0; i < iattrCnt; i++)
    {
        int k = 0;
        for(int k = 0; k < iattrCnt; k++)
        {
            if(memcmp(attrList[i].attrName, relattrs[k].attrName, sizeof(relattrs[k].attrName)))
                break;
        }
        cout << k << ' ';
    }


    //if no value is specified for an attribute in attriList
    return OK;
}
