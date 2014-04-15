#include "catalog.h"
#include "query.h"
#include "index.h"
#include <cstring>

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
void infoTodesc(attrInfo *from, AttrDesc *dest)
{

}

Status Operators::Select(const string & result,      // name of the output relation
	                 const int projCnt,          // number of attributes in the projection
		         const attrInfo projNames[], // the list of projection attributes
		         const attrInfo *attr,       // attribute used inthe selection predicate 
		         const Operator op,         // predicate operation
		         const void *attrValue)     // literal value in the predicate
{
    Status status;
    bool indexed = false; // true if indexed and op is EQ
    if(attr == NULL)
    {
        indexed = false;
    }
    else
    {
        AttrDesc tmpDesc;
        status = attrCat->getInfo(attr->relName,attr->attrName,tmpDesc);
        if(status != OK)
            error.print(status);
        if(tmpDesc.indexed && op == EQ)
            indexed = true;
        else indexed  = false;
    }
    AttrDesc newProjNames[projCnt]; 
    AttrDesc *Attr;// = new AttrDesc;
    int offset = 0;
    for(int i = 0; i < projCnt; i++)
    {
        AttrDesc tmpDesc;
        status = attrCat->getInfo(projNames[i].relName,projNames[i].attrName,tmpDesc);
        if(status != OK)
            error.print(status);
        newProjNames[i].indexed = false;
        newProjNames[i].attrOffset = offset;
        offset += tmpDesc.attrLen;
        newProjNames[i].attrLen = tmpDesc.attrLen;
        newProjNames[i].attrType = projNames[i].attrType;
        memcpy(newProjNames[i].relName,projNames[i].relName,strlen(projNames[i].relName)+1);
        memcpy(newProjNames[i].attrName,projNames[i].attrName, strlen(projNames[i].attrName)+1);
    }
    AttrDesc tmpDesc;
    if(attr != NULL)
    {
        status = attrCat->getInfo(attr->relName,attr->attrName,tmpDesc);
        if(status != OK)
            error.print(status);
        Attr = &tmpDesc;
    }
    else Attr = NULL;
    if(indexed)
        IndexSelect(result,projCnt,newProjNames,Attr,op,attrValue,offset);
    else
        ScanSelect(result,projCnt,newProjNames,Attr,op,attrValue,offset);
    cout << "select OK\n";
    return OK;
}

