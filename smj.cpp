#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"

/* Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  cout << "Algorithm: SM Join" << endl;

  /* Your solution goes here */

    Status status;
/*    HeapFileScan tmpfile1(attrDesc1.relName,status);
    if(status != OK)
        error.print(status);
    HeapFileScan tmpfile2(attrDesc2.relName,status);
    if(status != OK)
        error.print(status);
*/  HeapFile tmpResult(result,status);
    if(status != OK)
        error.print(status);

    int max1 = 0;
    int max2 = 0;
    int attrnum = 0;
    AttrDesc *tmpdesc;
    attrCat->getRelInfo(attrDesc1.relName,attrnum,tmpdesc);
    max1 = tmpdesc[attrnum-1].attrOffset + tmpdesc[attrnum-1].attrLen;
    delete [] tmpdesc;
    attrCat->getRelInfo(attrDesc2.relName,attrnum,tmpdesc);
    max2 = tmpdesc[attrnum-1].attrOffset + tmpdesc[attrnum-1].attrLen;
    delete [] tmpdesc;

    max1 = BufMgr::numUnpinnedPages*0.8*1024/max1;
    max2 = BufMgr::numUnpinnedPages*0.8*1024/max2;

    SortedFile tmpsort1(attrDesc1.relName,attrDesc1.attrOffset,attrDesc1.attrLen,attrDesc1.attrType,max1,status);
    if(status != OK)
        error.print(status);
    SortedFile tmpsort2(attrDesc2.relName,attrDesc2.attrOffset,attrDesc2.attrLen,attrDesc2.attrType,max2,status);
    if(status != OK)
        error.print(status);



 /*   if(attrDesc == NULL)
    {
        //get all the tuples;
        tmpfile.startScan(0,1,DOUBLE,NULL,op);
    }
    else
    {
        //scan the heapfile to get qualify tuples
        tmpfile.startScan(attrDesc->attrOffset,attrDesc->attrLen,static_cast<Datatype>(attrDesc->attrType),(char *)attrValue,op);
    }
*/
    Record rec1, rec2;
    status = tmpsort2.next(rec2);
    if(status != OK)
        return OK;
    tmpsort2.setMark();
    int match;
    do{
        status = tmpsort1.next(rec1);
        if(status != OK)
            return OK;
        match = Operators::matchRec(rec1,rec2,attrDesc1,attrDesc2);
    }while (match < 0);


    RID outRid;
    while(true)
    {
        tmpsort2.gotoMark();
        tmpsort2.next(rec2);

        match = Operators::matchRec(rec1,rec2,attrDesc1,attrDesc2);
        while ( match >= 0 )
        {
            if ( match == 0 )
            {
                
                Record tmpRec;
                tmpRec.data = new char[reclen];
                tmpRec.length = reclen;
                int offset = 0;
        
                for(int i = 0; i < projCnt; i++)
                {   
                    AttrDesc tmpDesc;
                    attrCat->getInfo(attrDescArray[i].relName,attrDescArray[i].attrName,tmpDesc);
                    if (attrDescArray[i].relName == attrDesc1.relName) 
                        memcpy(tmpRec.data + attrDescArray[i].attrOffset, rec1.data + tmpDesc.attrOffset, attrDescArray[i].attrLen);
                    else
                        memcpy(tmpRec.data + attrDescArray[i].attrOffset, rec2.data + tmpDesc.attrOffset, attrDescArray[i].attrLen);
                    offset += attrDescArray[i].attrLen;
                }
                RID tmpRid;
                tmpResult.insertRecord(tmpRec,tmpRid);
                delete tmpRec.data;
                
                status = tmpsort2.next(rec2);
                if(status != OK)
                    break;
            }
            else
            {
                status = tmpsort2.next(rec2);
                if(status != OK)
                    return OK;
                tmpsort2.setMark();
            }
            
            match = Operators::matchRec(rec1,rec2,attrDesc1,attrDesc2);
        }

        status = tmpsort1.next(rec1);
        if(status != OK)
            break;

    }

  return OK;
}

