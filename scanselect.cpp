#include "catalog.h"
#include "query.h"
#include "index.h"
#include <cstring>

/* 
 * A simple scan select using a heap file scan
 */

Status Operators::ScanSelect(const string& result,       // Name of the output relation
                             const int projCnt,          // Number of attributes in the projection
                             const AttrDesc projNames[], // Projection list (as AttrDesc)
                             const AttrDesc* attrDesc,   // Attribute in the selection predicate
                             const Operator op,          // Predicate operator
                             const void* attrValue,      // Pointer to the literal value in the predicate
                             const int reclen)           // Length of a tuple in the result relation
{
  cout << "Algorithm: File Scan" << endl;
  
  /* Your solution goes here */
  //if attrDesc = NULL
    Status status;
    HeapFileScan tmpfile(projNames[0].relName,status);
    if(status != OK)
        error.print(status);
    HeapFile tmpResult(result,status);
    if(status != OK)
        error.print(status);

    if(attrDesc == NULL)
    {
        //get all the tuples;
        tmpfile.startScan(0,1,DOUBLE,NULL,op);
    }
    else
    {
        //scan the heapfile to get qualify tuples
        tmpfile.startScan(attrDesc->attrOffset,attrDesc->attrLen,static_cast<Datatype>(attrDesc->attrType),(char *)attrValue,op);
    }

    RID outRid;
    Record rec;
    while(true)
    {
        status = tmpfile.scanNext(outRid,rec);
        if(status != OK)
            break;

        Record tmpRec;
        tmpRec.data = new char[reclen];
        tmpRec.length = reclen;
        int offset = 0;
        for(int i = 0; i < projCnt; i++)
        {
            AttrDesc tmpDesc;
            attrCat->getInfo(projNames[i].relName,projNames[i].attrName,tmpDesc);
            
            memcpy(tmpRec.data + projNames[i].attrOffset, rec.data + tmpDesc.attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }
        RID tmpRid;
        tmpResult.insertRecord(tmpRec,tmpRid);
        delete tmpRec.data;

    }

    return OK;
}
