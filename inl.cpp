#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cstring>

/* 
 * Indexed nested loop evaluates joins with an index on the 
 * inner/right relation (attrDesc2)
 */

Status Operators::INL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // Length of a tuple in the output relation
{
  cout << "Algorithm: Indexed NL Join" << endl;

  /* Your solution goes here */
    Status status;
    HeapFileScan lfile(attrDesc1.relName,status);
    if(status != OK)
        error.print(status);

    HeapFileScan rfile(attrDesc2.relName,status);
    if(status != OK)
        error.print(status);

    //
    Index rindex(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType),0,status);
    if(status != OK)
        error.print(status);
    HeapFile tmpResult(result,status);
    if(status != OK)
        error.print(status);

    //
    //status = index.startScan(
    status = lfile.startScan(0,1,DOUBLE,NULL,op);
    if(status != OK)
        error.print(status);

    RID outRid;
    Record outRec;
    while(true)
    {
        status = lfile.scanNext(outRid,outRec);
        if(status != OK)
            break;

        //for every tuples in the out relation, check the index of the inner relation
        char *value  = new char[attrDesc1.attrLen];
        memcpy(value,(char *)outRec.data + attrDesc1.attrOffset,attrDesc1.attrLen);
        status = rindex.startScan(value);
        if(status != OK)
            error.print(status);

        Record inRec;
        RID inRid;
        while(true)
        {
            status = rindex.scanNext(inRid);
            if(status != OK)
                break;
            //found one
            status = rfile.getRandomRecord(inRid,inRec);
            //all we need might be in the index
            Record rec;
            rec.data = new char[reclen];
            rec.length = reclen;

            for(int i = 0; i < projCnt; i++)
            {
                
                if(memcmp(attrDescArray[i].attrName,attrDesc1.attrName,strlen(attrDesc1.attrName))==0)
                {
                    //attr in the first relation
                    AttrDesc tmpDesc;
                    attrCat->getInfo(attrDescArray[i].relName,attrDescArray[i].attrName,tmpDesc);//really need?

                    memcpy(rec.data + attrDescArray[i].attrOffset,outRec.data + tmpDesc.attrOffset,attrDescArray[i].attrLen);

                }
                else
                {
                    //attr in the right relation
                    AttrDesc tmpDesc;
                    attrCat->getInfo(attrDescArray[i].relName,attrDescArray[i].attrName,tmpDesc);//really need?

                    memcpy(rec.data + attrDescArray[i].attrOffset,inRec.data + tmpDesc.attrOffset,attrDescArray[i].attrLen);

                }
            }
            //copied all the data into rec
            RID tmpRidd;
            tmpResult.insertRecord(rec,tmpRidd);
            delete (char *) rec.data;
        
        }
        rindex.endScan();

    }


  return OK;
}

