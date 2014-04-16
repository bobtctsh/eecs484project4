#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cstring>

Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  cout << "Algorithm: Simple NL Join" << endl;
    
  /* Your solution goes here */

    Status status;
    HeapFileScan lfile(attrDesc1.relName,status);
    if(status != OK)
        error.print(status);
    HeapFile tmpResult(result,status);
    if(status != OK)
        error.print(status);

    //start scan
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
        HeapFileScan rfile(attrDesc2.relName,0,1,DOUBLE,NULL,op,status);
        if(status != OK)
            error.print(status);
        //for every tuple in lfile, scan rfile
        Record inRec;
        RID inRid;
        while(true)
        {
            status = rfile.scanNext(inRid,inRec);
            if(status != OK)
                break;
            //found one
            Record rec;

            int match = matchRec(outRec,inRec,attrDesc1,attrDesc2);

            if((match == 0 && (op == EQ || op == GTE || op == LTE))||(match < 0 && (op == LT || op == LTE))||(match > 0 && (op == GTE || op == GT)))
            {

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
           // rfile.endScan();
    }


  return OK;
}

