#include "catalog.h"
#include "query.h"
#include "index.h"
#include <cstring>
Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
  cout << "Algorithm: Index Select" << endl;
    

  /* Your solution goes here */
    Status status;
    Index tmpIndex(projNames[0].relName,attrDesc->attrOffset,attrDesc->attrLen,static_cast<Datatype>(attrDesc->attrType),0,status);
    if(status != OK)
        error.print(status);
    HeapFileScan tmpfile(projNames[0].relName,status);
    if(status != OK)
        error.print(status);

    status = tmpIndex.startScan(attrValue);
    if(status != OK)
        error.print(status);
    HeapFile tmpResult(result,status);
    if(status != OK)
        error.print(status);

    RID outRid;
    Record rec;

    while(true)
    {
        status = tmpIndex.scanNext(outRid);
        if(status != OK)
            break;

        status = tmpfile.getRandomRecord(outRid,rec);
        if(status != OK)
            error.print(status);
        //value in the index?
        int offset = 0;
        Record tmpRec;
        tmpRec.data = new char[reclen];
        tmpRec.length = reclen;

        for(int i = 0; i < projCnt; i ++)
        {
            AttrDesc tmpDesc;
            attrCat->getInfo(projNames[i].relName,projNames[i].attrName,tmpDesc);

            memcpy(tmpRec.data + projNames[i].attrOffset, rec.data + tmpDesc.attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }
        RID tmpRid;

        tmpResult.insertRecord(tmpRec,tmpRid);
        delete (char *)tmpRec.data;


    }

    return OK;
}

