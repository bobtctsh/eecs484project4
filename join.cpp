#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cmath>
#include <cstring>

#define MAX(a,b) ((a) > (b) ? (a) : (b))
#define DOUBLEERROR 1e-07

/*
 * Joins two relations
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Operators::Join(const string& result,           // Name of the output relation 
                       const int projCnt,              // Number of attributes in the projection
    	               const attrInfo projNames[],     // List of projection attributes
    	               const attrInfo* attr1,          // Left attr in the join predicate
    	               const Operator op,              // Predicate operator
    	               const attrInfo* attr2)          // Right attr in the join predicate
{
    /* Your solution goes here */
    Status status;

    //chhose which algorithn to run
    int alg;// 0--snl
            // 1--inl
            // 2--sml

    if(op != EQ)
        alg = 0;
    else
    {
        AttrDesc tmpDesc;
        attrCat->getInfo(attr1->relName,attr1->attrName,tmpDesc);
        if(tmpDesc.indexed)
        {
            alg = 1;
            //use inl
            //garentee the second has index
            const attrInfo *tmp = attr2;
            attr2 = attr1;
            attr1 = tmp;
        }
        else
        {
            attrCat->getInfo(attr2->relName,attr2->attrName,tmpDesc);
            if(tmpDesc.indexed)
            {
                alg = 1;
                //use inl
            }
            else
            {
                //equi join, no index, use smj
                alg = 2;
            }
        }
    }
    //transform attrinfo to AttrDesc
    AttrDesc newProjNames[projCnt];
    //AttrDesc *lattr, *rattr;
    int offset = 0;

    for(int i = 0; i < projCnt; i++)
    {
        AttrDesc tmpDesc;
        status = attrCat->getInfo(projNames[i].relName,projNames[i].attrName,tmpDesc);
        if(status != OK)
            error.print(status);
        newProjNames[i].indexed = false;
        newProjNames[i].attrOffset = offset;
        offset +=tmpDesc.attrLen;
        newProjNames[i].attrLen = tmpDesc.attrLen;
        newProjNames[i].attrType = projNames[i].attrType;
        memcpy(newProjNames[i].relName,projNames[i].relName,strlen(projNames[i].relName)+1);
        memcpy(newProjNames[i].attrName,projNames[i].attrName,strlen(projNames[i].attrName)+1);
    }
    AttrDesc ldesc,rdesc;
    status = attrCat->getInfo(attr1->relName,attr1->attrName,ldesc);
    if(status != OK)
        error.print(status);
    status = attrCat->getInfo(attr2->relName,attr2->attrName,rdesc);
    if(status != OK)
        error.print(status);
    //lattr = &ldesc;
    //rattr = &rdesc;

    switch(alg)
    {
        case 0: status = SNL(result, projCnt, newProjNames, ldesc, op, rdesc, offset);
            break;
        case 1: status = INL(result, projCnt, newProjNames, ldesc, op, rdesc, offset);
            break;
        case 2: status = SMJ(result, projCnt, newProjNames, ldesc, op, rdesc, offset);
            break;
    }
    if(status != OK)
        error.print(status);

	return OK;
}

// Function to compare two record based on the predicate. Returns 0 if the two attributes 
// are equal, a negative number if the left (attrDesc1) attribute is less that the right 
// attribute, otherwise this function returns a positive number.
int Operators::matchRec(const Record& outerRec,     // Left record
                        const Record& innerRec,     // Right record
                        const AttrDesc& attrDesc1,  // Left attribute in the predicate
                        const AttrDesc& attrDesc2)  // Right attribute in the predicate
{
    int tmpInt1, tmpInt2;
    double tmpFloat1, tmpFloat2, floatDiff;

    // Compare the attribute values using memcpy to avoid byte alignment issues
    switch(attrDesc1.attrType)
    {
        case INTEGER:
            memcpy(&tmpInt1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(int));
            memcpy(&tmpInt2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(int));
            return tmpInt1 - tmpInt2;

        case DOUBLE:
            memcpy(&tmpFloat1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(double));
            memcpy(&tmpFloat2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(double));
            floatDiff = tmpFloat1 - tmpFloat2;
            return (fabs(floatDiff) < DOUBLEERROR) ? 0 : (floatDiff < 0?floor(floatDiff):ceil(floatDiff));


        case STRING:
            return strncmp(
                (char *) outerRec.data + attrDesc1.attrOffset, 
                (char *) innerRec.data + attrDesc2.attrOffset, 
                MAX(attrDesc1.attrLen, attrDesc2.attrLen));
    }

    return 0;
}
