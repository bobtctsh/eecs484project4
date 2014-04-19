#include <iostream>
#include <string.h>
#include "page.h"
#include <cstring>
#include "string.h"
using namespace std;

// page class constructor
// please initialize all private data members. Note that the
// page starts off empty, dummy should NOT be touched, and there
// are no initial entries in slot array.
void Page::init(int pageNo)
{
    /* Solution Here */
    curPage = pageNo; //initiate current page 
    freeSpace = PAGESIZE-DPFIXED;// free space in bytes
    freePtr = 0;
    //init slot
    slotCnt = -1;
    slot[0].length=-1;
    slot[0].offset=-1;
}

// dump page utlity
void Page::dumpPage() const
{
  int i;
  cout << "curPage = " << curPage <<", nextPage = " << nextPage
       << "\nfreePtr = " << freePtr << ",  freeSpace = " << freeSpace 
       << ", slotCnt = " << slotCnt << endl;
    
    for (i=0;i>slotCnt;i--)
      cout << "slot[" << i << "].offset = " << slot[i].offset 
	   << ", slot[" << i << "].length = " << slot[i].length << endl;
}

const int Page::getPrevPage() const
{
   return prevPage;
}

void Page::setPrevPage(int pageNo)
{
    prevPage = pageNo;
}

void Page::setNextPage(int pageNo)
{
    nextPage = pageNo;
}

const int Page::getNextPage() const
{
    return nextPage;
}

const short Page::getFreeSpace() const
{
    /* Solution Here */
    return freeSpace;
}
    
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns NOSPACE if sufficient space does not exist.
// RID of the new record is returned via rid parameter.
// When picking a slot first check tooffset see if any spots are avaialable 
// in the middle of the slot array. Look from least negative to most 
// negative.

const Status Page::insertRecord(const Record & rec, RID& rid)
{
    /* Solution Here */
    //check different size
    //wheter there is enough space for the new record
    if(freeSpace <rec.length) 
    {
        return NOSPACE;
    }
    //slot[i] where i is negative to get the slot
    for(int i = 0 ; i> slotCnt; i --)
    {
        if(slot[i].length==-1)
        {
            //find slot i which is unused
            slot[i].offset=freePtr;
            slot[i].length = rec.length;
            rid.pageNo= curPage;
            rid.slotNo=-i;
            //insert the new record
            memcpy(data+freePtr,(char*)rec.data,rec.length);
    
            freePtr +=rec.length;
            freeSpace -= rec.length;

            //evertthing done.
            return OK;
        }
    }
    // didnt find an avaliable slot, need to to assign a new one
    // check to see if there is enough space for the record and the new slot
    if(freeSpace <(short)rec.length + (short)sizeof(slot_t)) return NOSPACE;

    //bookkeeping,new slot 'slotCnt' is used now
    slot[slotCnt].offset = freePtr;
    slot[slotCnt].length = rec.length;
    rid.pageNo=curPage;
    rid.slotNo = -slotCnt;
    memcpy(data+freePtr,(char*)rec.data,rec.length);
    freePtr += rec.length;
    freeSpace -=rec.length + sizeof(slot_t);
    slotCnt--;

    return OK;
}


// delete a record from a page. Returns OK if everything went OK,
// if invalid RID passed in return INVALIDSLOTNO
// if the record to be deleted is last record on page return NORECORDS
// compacts remaining records but leaves hole in slot array
// use bcopy and not memcpy when shifting overlapping memory. 
const Status Page::deleteRecord(const RID & rid)
{
    /* Solution Here */
    // is rid valid here
    if(rid.pageNo != curPage || rid.slotNo>(- slotCnt) || rid.slotNo <0)
    {
        return INVALIDSLOTNO;
    }

    short tmpSlot = -rid.slotNo;
    //empty the data;
    memset(data+slot[tmpSlot].offset,0,slot[tmpSlot].length);
    //need to compact after delete
    short dest = slot[tmpSlot].offset;
    short src = dest + slot[tmpSlot].length;
    //
    while(true)
    {
        if(src == freePtr)
            break;
        int k;
        for(k = 0 ; k > slotCnt ; k--)
        {
            if(slot[k].offset == src)
            {
                slot[k].offset = dest;
                break;
            }
        }
        //move the record
        bcopy(data+src,data+dest,slot[k].length);
        dest += slot[k].length;
        src += slot[k].length;
    }
    // bookkeeping 
    freePtr -= slot[tmpSlot].length;
    freeSpace += slot[tmpSlot].length;
    slot[-rid.slotNo].length=-1;
    slot[-rid.slotNo].offset =-1;
    // are there any record in this page?
    for(int i = 0 ; i > slotCnt ; i--)
    {
        if(slot[i].length !=-1)
        {
            return OK;
        }
    }
    return NORECORDS; 
}

// returns RID of first record on page
// return OK on success and NORECORDS if no valid RID in page
const Status Page::firstRecord(RID& firstRid) const
{
    /* Solution Here */
    for(int i = 0 ; i > slotCnt ; i--)
    {
        if(slot[i].length !=-1)//start from slot[0] to slot[slotCnt]
        {
            firstRid.pageNo=curPage;
            firstRid.slotNo=-i;
            //find the first one
            return OK;
        }
    }
    return NORECORDS; 

}

// returns RID of next record on the page
// returns ENDOFPAGE if no more records exist on the page; otherwise OK
const Status Page::nextRecord (const RID &curRid, RID& nextRid) const
{
    /* Solution Here */
    //check Rid
    //no more records exist on the page
    if(curRid.pageNo == -1)
    {
        return ENDOFPAGE;
    }
    //whethre curRid is invalid here
    if(curRid.pageNo != curPage || curRid.slotNo>(- slotCnt) || curRid.slotNo <0)
    {
        return INVALIDSLOTNO;
    }

    for(int i = -curRid.slotNo-1; i> slotCnt ; i--)
    {
        if(slot[i].length != -1)
        {
            nextRid.pageNo= curPage;
            nextRid.slotNo= -i;
            return OK;
        }
    }
    return ENDOFPAGE;
}

// returns length and pointer to record with RID rid
// returns OK on success and INVALIDSLOTNO if invalid rid 
const Status Page::getRecord(const RID & rid, Record & rec)
{
    /* Solution Here */ 
    if(rid.pageNo != curPage || rid.slotNo>(- slotCnt) || rid.slotNo <0)
    {
        return INVALIDSLOTNO;
    }
    short offset = slot[-rid.slotNo].offset;
    short length = slot[-rid.slotNo].length;
    char *dataRec = new char [length];
    //
    memcpy(dataRec,data+offset,length);
    rec.length = length;
    rec.data = (void*) dataRec;
    return OK;
}
