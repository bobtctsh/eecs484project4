#include "catalog.h"
#include "query.h"
#include "index.h"
#include <cstring>
#include <vector>
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
    Status status;
    vector<int> maping; // deal with the uncorrect order
    attrCat->getRelInfo(relation, iattrCnt, relattrs);

    if(attrCnt != iattrCnt)
        ;//number of attr doesnt match

    std::cout << relation << ' ' << *(int *)(attrList[0].attrValue) << endl;
    std::cout << "GG: " << relattrs[0].relName << endl;
    std::cout << "L: " <<  attrList[0].attrName << MAXNAME <<  endl;
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
        for(k = 0; k < iattrCnt; k++)
        {
            if(memcmp(attrList[k].attrName, relattrs[i].attrName, strlen(relattrs[i].attrName))==0)
                break;
        }
        maping.push_back(k);
        memcpy(irecord.data + relattrs[i].attrOffset, attrList[k].attrValue, relattrs[i].attrLen);
    }
    //init file
    HeapFile tmpfile(relation, status);

    if(status != OK)
        error.print(status);
    RID outRid;
    status = tmpfile.insertRecord(irecord,outRid);

    if(status != 0)
        error.print(status);

    //insert record ID into index
    for(int i = 0; i < iattrCnt; i++)
    {
        if(relattrs[i].indexed != 0)
        {
            //this attr is indexed
            Index tmpIndex(relation,relattrs[i].attrOffset,relattrs[i].attrLen,static_cast<Datatype>(relattrs[i].attrType),1,status);
            if(status != OK)
                error.print(status);

            tmpIndex.insertEntry(attrList[maping[i]].attrValue,outRid);

        }
    }

    cout << outRid.pageNo << endl;



    //if no value is specified for an attribute in attriList
    return OK;
}
