/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"



//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
/**
 * BTreeIndex constructor is used to initalize a B-Tree.
 * This evaluates the presence of the passed in index file. If it already does,
 * then open the file. Else, create a new one and insert entries accordingly.
 *
 * @param relationName      Name of the file to be used.
 * @param outIndexName      Name of the index file.
 * @param bufMgrIn          Global buffer manager instance.
 * @param attrByteOffset    The byte offset of the attribute in the tuple used to
 *                          build the index.
 * @param attrType          The data type of the indexed attribute.
 */
BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
        // Global scanning variable used to check if a scan is in progress.
        // Initialize as false.
        scanExecuting = false;

        // Global initializations of the node and leaf occupancies.
        leafOccupancy = INTARRAYLEAFSIZE;
        nodeOccupancy = INTARRAYNONLEAFSIZE;

        // Initialize the buffer manager.
        bufMgr = bufMgrIn;

        // Name the index file accordingly.
        std::ostringstream idxStr;
        idxStr << relationName << "." << attrByteOffset;
        outIndexName = idxStr.str();

        // Try to see if the file already exists. If it does, then update the
        // B-Tree's metaInfo.
        try
        {
            file = new BlobFile(outIndexName, false);
            Page *header;
            headerPageNum = file->getFirstPageNo();
//            cout << "Reading the page" << endl;
            bufMgr->readPage(file, headerPageNum, header);
//             cout << "Page read successfully" << endl;
            IndexMetaInfo* metaInfo = (IndexMetaInfo*) header;
            // Update the rootPageNum to reflect the information stored in the file.
            rootPageNum = metaInfo->rootPageNo;
            bufMgr->unPinPage(file, headerPageNum, false);
        }
        // If the file was not found (or nonexistent), we open a new one and allocate a new
        // header and root page.
        catch(FileNotFoundException e)
        {
            file = new BlobFile(outIndexName, true);
            // Check to see if the blobfile was successfully created.
//            if (file != nullptr) {
//                cout << "Successful file allocation" << endl;
//            }
            // allocate root and header page
            Page* header;
            Page* root;
//            cout << "Allocating space for header and root pages" << endl;
            bufMgr->allocPage(file, headerPageNum, header);
            bufMgr->allocPage(file, rootPageNum, root);
//            cout << "Space successfully allocated" << endl;

            // Update global var firstRootNum to keep track of the original root
            // page value and update its sibling.
            firstRootNum = rootPageNum;
            LeafNodeInt* rootUpdate = (LeafNodeInt*) root;
            rootUpdate->rightSibPageNo = 0;

            // Update meta information for the newly created file.
            IndexMetaInfo* metaInfo = (IndexMetaInfo*) header;
            strncpy((char*)(&(metaInfo->relationName)), relationName.c_str(), relationName.length());
            // Make sure copy was successful.
//            cout << relationName.c_str() << endl;
//            cout << metaInfo->relationName << endl;
            metaInfo->attrByteOffset = attrByteOffset;
            metaInfo->attrType = attrType;
            metaInfo->rootPageNo = rootPageNum;

            // Unpin the header and root pages to free up space before the scan.
            bufMgr->unPinPage(file, headerPageNum, true);
            bufMgr->unPinPage(file, rootPageNum, true);

            // Fill the newly created blobfile using filescan.
            FileScan fileScan(relationName, bufMgr);
            RecordId rid;
            try
            {
                while(1)
                {
                    fileScan.scanNext(rid);
                    std::string record = fileScan.getRecord();
                    insertEntry(record.c_str() + attrByteOffset, rid);
                }
            }
            catch(EndOfFileException e)
            {
                // save Btree index file to disk
                bufMgr->flushFile(file);
            }
        }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
/**
 * This destructor method flushes the B-Tree index file and unpins any pages
 * associated with it. It also sets the global scanning variable to false so
 * other methods don't try to keep populating a blobfile that doesn't exist.
 */
BTreeIndex::~BTreeIndex()
{
    // Flush the file, deconstruct the file, free the file object.
    bufMgr->flushFile(file);
    delete file;
    file = nullptr;
//    cout << file->getFirstPageNo() << endl;
    // End the scan after everything has been freed up
    scanExecuting = false;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
/**
 * This method inserts a new entry into the index by traversing through the tree
 * to find a leaf to insert the RIDKeyPair <key,rid>. Most of the work is performed
 * by helper methods so that recursive calls can be used to traverse and split the
 * nodes if need be.
 * @param key   Pointer to the integer we want to insert.
 * @param rid   Corresponding record id of the tuple.
 */
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
    // Create the entry to add to the tree
    RIDKeyPair<int> data;
    data.set(rid, *((int*) key));
    Page* root;
//    cout << "InsertEntry(): Reading page" << endl;
    bufMgr->readPage(file, rootPageNum, root);
//    cout << "InsertEntry(): Page read" << endl;
    // Create the PageKeyPair object to add to the tree if a split is required.
    PageKeyPair<int>* child = nullptr;
    // Check to see if the root has changed since the B-Tree initialization.
    if (rootPageNum != firstRootNum) {
        findSpace(root, rootPageNum, data, 0, child);
    } else {
        findSpace(root, rootPageNum, data, 1, child);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::findSpace
// -----------------------------------------------------------------------------
/**
 * This method attempts to find space in the file to insert values by utilizing
 * recursive calls to search through the nodes in the tree.
 * @param curr      The current page being searched through.
 * @param currNum   The pageid of the current page being searched through.
 * @param data      The entry to be inserted into the tree.
 * @param level     The value indicating whether or not a node is a branch or a leaf.
 * @param child     Placeholder for an entry that needs to be propogated up the
 *                  tree. Only utilized when splitting a node.
 */
const void BTreeIndex::findSpace(Page* currPage, PageId currNum, RIDKeyPair<int> data,
                                 int level, PageKeyPair<int>* &child) {
    // If the current node is a leaf, then add the entry
    if (level == 1) {
        LeafNodeInt *leaf = (LeafNodeInt *) currPage;
        // If the leaf node has room, add the data.
        if (leaf->ridArray[leafOccupancy - 1].page_number == 0) {
            addToLeaf(leaf, data);
            bufMgr->unPinPage(file, currNum, true);
            child = nullptr;
        } else {
            leafSplit(child, leaf, currNum, data);
        }
    // Else, propogate down to the right place to insert the node
    } else {
        NonLeafNodeInt *curr = (NonLeafNodeInt *)currPage;
        Page *next;
        // Find the right location in the current branch node to insert the key.
        int i = nodeOccupancy;
        // While the pages are uninitialized..
        while(i >= 0 && (curr->pageNoArray[i] == 0))
        {
            i--;
        }
        // While the key is less than the keys already inserted..
        while(i > 0 && (curr->keyArray[i-1] > data.key))
        {
            i--;
        }
        // The next node to check
        PageId nextNum = curr->pageNoArray[i];
//        cout << "FindSpace(): Reading page." << endl;
        bufMgr->readPage(file, nextNum, next);
//        cout << "FindSpace(): Read successful." << endl;
        // If the current node is not a leaf node
        if (curr->level != 1) {
            findSpace(next, nextNum, data, 0, child);
        // If the current node is a leaf node
        } else {
            findSpace(next, nextNum, data, 1, child);
        }
        // If the child is non-null, add the entry to a branch node.
        if (child != nullptr) {
            // Check to see if we can insert the key into this node.
            if (curr->pageNoArray[nodeOccupancy] == 0) {
                addToBranch(curr, child);
                // Free the pointer after adding the data.
                child = nullptr;
                bufMgr->unPinPage(file, currNum, true);
            // Split is required because of full node.
            } else {
                branchSplit(child, curr, currNum);
            }
        // Free the page because the data has been added.
        } else {
            bufMgr->unPinPage(file, currNum, false);
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::addToBranch
// -----------------------------------------------------------------------------
/**
 * This method iterates through a branch node structure and finds the right place
 * to enter a new data node.
 * @param branch    The branch to be added to.
 * @param data      The data to add to said branch node.
 */
const void BTreeIndex::addToBranch(NonLeafNodeInt* branch, PageKeyPair<int>* data) {
    int i = nodeOccupancy;  // Index used for iteration through the page number and key arrays.
    // Go through all of the empty spots in the branch.
    while ((branch->pageNoArray[i] == 0) && i >= 0) {
        i--;
    }
    // If not at index 0, go ahead and find the right place to insert the data entry,
    // shifting the other items in the array if needed.
    while ((data->key < branch->keyArray[i-1]) && i > 0) {
        branch->pageNoArray[i+1] = branch->pageNoArray[i];
        branch->keyArray[i] = branch->keyArray[i-1];
        i--;
    }
    // Update branch arrays with the new data
    branch->pageNoArray[i+1] = data->pageNo;
    branch->keyArray[i] = data->key;
}

// -----------------------------------------------------------------------------
// BTreeIndex::addToLeaf
// -----------------------------------------------------------------------------
/**
 * This method iterates through a leaf node structure and finds the right place to
 * insert the data read from the file.
 * @param leaf      The leaf node to be added to.
 * @param data      The data to add to said leaf node.
 */
const void BTreeIndex::addToLeaf(LeafNodeInt* leaf, RIDKeyPair<int> data) {
    // Check to see if the leaf page is empty
    if (leaf->ridArray[0].page_number != 0) {
        int i = leafOccupancy - 1;
        // While the pages are uninitialized..
        while(i >= 0 && (leaf->ridArray[i].page_number == 0)) {
            i--;
        }
        // While the key is less than the keys already inserted..
        while(i >= 0 && (leaf->keyArray[i] > data.key)) {
            leaf->keyArray[i+1] = leaf->keyArray[i];
            leaf->ridArray[i+1] = leaf->ridArray[i];
            i--;
        }
        // insert entry
        leaf->keyArray[i+1] = data.key;
        leaf->ridArray[i+1] = data.rid;
    } else {
        leaf->keyArray[0] = data.key;
        leaf->ridArray[0] = data.rid;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::newRoot
// -----------------------------------------------------------------------------
/**
 * This method updates the root of the B-Tree. In this case, the root needs to be split
 * and entries pushed up the tree.
 * @param firstNode     This is the page number of the first entry of the root.
 * @param child         This is the entry that needs to be propogated upwards
 *                      after the split occurs.
 */
const void BTreeIndex::newRoot(PageId firstNode, PageKeyPair<int>* child) {
    // New root with metadata updates
    Page* newRoot;
    PageId newNum;
//    cout << "newRoot(): Allocating space for a new root page." << endl;
    bufMgr->allocPage(file, newNum, newRoot);
//    cout << "newRoot(): Space allocated for new root page." << endl;
    NonLeafNodeInt* newPage = (NonLeafNodeInt*) newRoot;    // New page allocated for root.
    if (firstRootNum == rootPageNum) {
        newPage->level = 1;     // If the root is a leaf
    } else {
        newPage->level = 0;
    }
    // Update the root page metadata to reflect changes.
    newPage->keyArray[0] = child->key;
    newPage->pageNoArray[0] = firstNode;
    newPage->pageNoArray[1] = child->pageNo;
    // Update the header page
    Page* newMetaInfo;
//    cout << "newRoot(): Reading in page." << endl;
    bufMgr->readPage(file, headerPageNum, newMetaInfo);
//    cout << "newRoot(): Page read successful." << endl;
    IndexMetaInfo* metaInfo = (IndexMetaInfo*) newMetaInfo;
    metaInfo->rootPageNo = newNum;
    rootPageNum = newNum;       // Update the global root page variable.
    // Free pages from buffer so data overflow doesn't occur.
    bufMgr->unPinPage(file, headerPageNum, true);
    bufMgr->unPinPage(file, newNum, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::branchSplit
// -----------------------------------------------------------------------------
/**
 * This method splits the branch so that the child entry can be entered into the tree.
 * First, a new branch page is allocated memory, then half the array is copied over from
 * the old branch node to the new one. Changes are made to the array to reflect this
 * split.
 * @param child     The entry that will need to be entered after the splitting occurs.
 * @param old       The node that will be split in this function.
 * @param oldNum    PageId that was used to index the node to be split.
 */
const void BTreeIndex::branchSplit(PageKeyPair<int>* &child, NonLeafNodeInt* old,
                                   PageId oldNum) {
    Page* newBranch;
    PageId newNum;
    PageKeyPair<int> newEntry;  // Keeps track of the entry to be added.
    bufMgr->allocPage(file, newNum, newBranch);
    NonLeafNodeInt* node = (NonLeafNodeInt*) newBranch;
    int split = nodeOccupancy/2;  // Indexes the midpoint of the array to be split at.
//    cout << "Number of nodes: " << split << endl;
    int index = split;      // Used to index the current position being propogated.
    // Even value with 0-based array requires index correction if key
    // lies in the former half of the array.
    if (child->key < old->keyArray[split] && nodeOccupancy%2 == 0) {
        index = split - 1;
    }
    newEntry.set(newNum, old->keyArray[index]);
    split = index + 1;
    // Copy over half of the node to the new node to reflect split
    for (int i = split; i < nodeOccupancy; i++) {
        // Update the page number arrays
        node->pageNoArray[i-split] = old->pageNoArray[i+1];
        old->pageNoArray[i+1] = (PageId) 0;
        // Update the key arrays
        node->keyArray[i-split] = old->keyArray[i];
        old->keyArray[i+1] = 0;
    }
    // Make sure to change information of the entry being propogated upwards.
    node->level = old->level;
    old->pageNoArray[index] = (PageId) 0;
    old->keyArray[index] = 0;

    // Determine where to put the new data entry.
    if (child->key < node->keyArray[0]) {
        addToBranch(old, child);
    } else {
        addToBranch(node, child);
    }
    // Unpin the unused pages
    bufMgr->unPinPage(file, oldNum, true);
    bufMgr->unPinPage(file, newNum, true);

    child = &newEntry;
    if (rootPageNum == oldNum) {
        newRoot(oldNum, child);
    }

}

// -----------------------------------------------------------------------------
// BTreeIndex::leafSplit
// -----------------------------------------------------------------------------
/**
 * This method splits a leaf so that the child entry can be added into the tree.
 * First, a new leaf node is allocated memory, then half of the old leaf's array
 * is copied over to the new one. Sibling pointers are properly changed and the
 * new data to be entered is added to the correct leaf node. If need be, update
 * the root.
 * @param child     The data that needs to be propogated upwards in the tree.
 * @param old       The old leaf node to be split.
 * @param oldNum    The page number of the old leaf node to be split.
 * @param data      The data entry to be added to the tree.
 */
const void BTreeIndex::leafSplit(PageKeyPair<int>* &child, LeafNodeInt* old, PageId oldNum,
                                 RIDKeyPair<int> data) {
    Page* newLeaf;      // Initialize a new leaf node for the split
    PageId newNum;      // Initialize a new leaf page ID for the split
//    cout << "leafSplit(): allocating new page" << endl;
    bufMgr->allocPage(file, newNum, newLeaf);
//    cout << "leafSplit(): new page allocated" << endl;
    LeafNodeInt* leafNode = (LeafNodeInt*) newLeaf;
    int split = leafOccupancy/2;    // Keep track of where to copy data from
    // If the key is greater than the value at the split, then increment.
    if (data.key > old->keyArray[split] && leafOccupancy%2 == 1) {
        split++;
    }
    // Propogate through and update the arrays after the split
    for (int i = split; i < leafOccupancy; i++) {
        // RID array updates
        leafNode->ridArray[i-split] = old->ridArray[i];
        old->ridArray[i].page_number = 0;
        // Key array updates
        leafNode->keyArray[i-split] = old->keyArray[i];
        old->keyArray[i] = 0;
    }
    // Make sure siblings are properly changed
    leafNode->rightSibPageNo = old->rightSibPageNo;
    old->rightSibPageNo = newNum;
    // Add to the old leaf node if they key is less than the middle value.
    if (data.key < old->keyArray[split-1]) {
//      cout << "LeafSplit(): adding to old node" << endl;
        addToLeaf(old, data);
    // Else, add it to the newly created leaf node
    } else {
//      cout << "LeafSplit(): adding to new node" << endl;
        addToLeaf(leafNode, data);
    }
    PageKeyPair<int> newPair;
    newPair.set(newNum, leafNode->keyArray[0]);
//    cout << newPair.pageNo << " " << newPair.key << endl;
    // Update the child pointer with the new data.
    child = &newPair;

    // Free up the buffer
    bufMgr->unPinPage(file, oldNum, true);
    bufMgr->unPinPage(file, newNum, true);

    // If the leaf that was split was the root, update the root.
    if (oldNum == rootPageNum) {
        newRoot(oldNum, child);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
/**
 * This method scans through the index for values indicated by the search
 * criteria. First, if a scan is already executing, it is ended. Once that
 * is done, the scan initializes at the root and searches through the tree
 * for values that match the search criteria. If the search criteria is valid
 * across multiple pages, then the current page is switched out for another,
 * and that new page is scanned through.
 * @param lowValParm    The low value to be tested.
 * @param lowOpParm     Operation used in testing the low range. (GT and GTE)
 * @param highValParm   The high value to be tested.
 * @param highOpParm    Operation used in testing the high range. (LT and LTE)
 * @throws BadScanrangeException    If the values passed in are invalid according
 *                                  to the tests here, error.
 * @throws BadOpCodesException      If the opcodes sent in are invalid, error.
 * @throws NoSuchKeyException       If the search does not yield any values, error.
 */
const void BTreeIndex::startScan(const void* lowValParm,
				                 const Operator lowOpParm,
				                 const void* highValParm,
				                 const Operator highOpParm)
{
    // End the previous scan before starting a new one
    if (scanExecuting == true) {
        endScan();
    }
    // Cast void* parameter to int* and dereference
    lowValInt = *((int*)lowValParm);
    lowOp = lowOpParm;
    highValInt = *((int*)highValParm);
    highOp = highOpParm;
    // Checking to see that the pointers are properly dereferenced
//    cout << "LowValInt = " << lowValInt << endl << "HighValInt = " << highValInt << endl;
    // Incorrect parameters, throw an exception
    if (lowValInt > highValInt) {
        throw BadScanrangeException();
    }
    if (lowOpParm == LT || lowOpParm == LTE || highOpParm == GT || highOpParm == GTE) {
//        cout << "LowOpParm = " << lowOpParm << endl << "HighOpParm = " << highOpParm << endl;
        throw BadOpcodesException();
    }
    // Read the root into the buffer
    currentPageNum = rootPageNum;
//    cout << "StartScan(): reading in page" << endl;
    // Read the root into the buffer
    bufMgr->readPage(file, currentPageNum, currentPageData);
//    cout << "StartScan(): page read successfully" << endl;
    // Check to see if the root is a leaf
    // If not, find the correct place to put the data
    int found = 0;
    // If the current rootPage is not the first initialized rootpage,
    // find the correct page to put the data into.
    if (!(firstRootNum == rootPageNum)) {
        NonLeafNodeInt* curr;
        while (found == 0) {
            curr = (NonLeafNodeInt*) currentPageData;
            // Current node is a leaf
            if (curr->level == 1) {
                found = 1;
            }
            PageId nextId;      // keeps track of the next page value
            int i = nodeOccupancy;      // indexing the amount of nodes
            // While the pages are uninitialized..
            while ((curr->pageNoArray[i] == 0) && i >= 0) {
                i--;
            }
            // While the keys are greater than the lowValInt..
            while ((curr->keyArray[i-1] >= lowValInt) && i > 0) {
                i--;
            }
            // Set the next page id to the correct index found above
            nextId = curr->pageNoArray[i];
            // Free buffer
            bufMgr->unPinPage(file, currentPageNum, false);
            currentPageNum = nextId;
            // Free buffer
            bufMgr->readPage(file, currentPageNum, currentPageData);
            // Check value of currentPageNum
//            cout << currentPageNum << endl;
        }
        found = 0;
    }
    // After the leaf to insert the data is found, propogate through and find the
    // correct place to index the data.
    while (found == 0) {
        // Cast the current page to a leaf node and iterate through
        LeafNodeInt* curr = (LeafNodeInt*) currentPageData;
        int i = 0;
        int insertOK = 0;
        while (i < leafOccupancy && insertOK == 0) {
            // Check to see if the next page has not been set and that the
            // arry is not full
            if (curr->ridArray[i+1].page_number == 0 && i < leafOccupancy - 1) {
                insertOK = 1;
            }
            int key = curr->keyArray[i];
            // Check to see if the correct leaf is found
            if (keyOpCodes(lowValInt, lowOp, highValInt, highOp, key) == 1) {
                found = 1;
                // Set scan to true
                scanExecuting = true;
                // Next entry indexed at i
                nextEntry = i;
                break;
            }
            if (insertOK == 1 || i == leafOccupancy - 1) {
                bufMgr->unPinPage(file, currentPageNum, false);
                // If the next entry has not been allocated, error.
                if (curr->rightSibPageNo == 0) {
                    throw NoSuchKeyFoundException();
                }
                // Check the next page
                currentPageNum = curr->rightSibPageNo;
//                cout << "StartScan(): reading page" << endl;
                bufMgr->readPage(file, currentPageNum, currentPageData);
//                cout << "StartScan(): read successful" << endl;
            }
            i++;
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------
/**
 * This method retrieves the record id of the next tuple matching the scan criteria.
 * If the page has been scanned, move onto its sibling (if one exists).
 * @param outRid    Record id of the next entry that matches the scan filter
 *                  set in startScan.
 * @throws IndexScanCompletedException  If there are no more records to go through,
 *                                      then this exception is thrown.
 * @throws ScanNotInitializedException  If a scan is not currently in progress, error.
 */
const void BTreeIndex::scanNext(RecordId& outRid)
{
    // Check to see if the global scanning variable has been set. If not, throw an
    // error.
    if (scanExecuting == false) {
        throw ScanNotInitializedException();
    }
    LeafNodeInt* curr = (LeafNodeInt*) currentPageData;
    // Check to see if the page is valid, then read through the page
    if (nextEntry == leafOccupancy || curr->ridArray[nextEntry].page_number == 0) {
        bufMgr->unPinPage(file, currentPageNum, false);
        // Next leaf is non-null
        if (curr->rightSibPageNo != 0) {
            currentPageNum = curr->rightSibPageNo;
//            cout << "scanNext(): Reading in page." << endl;
            bufMgr->readPage(file, currentPageNum, currentPageData);
//            cout << "scanNext(): Page read successfully." << endl;
            curr = (LeafNodeInt*) currentPageData;
            nextEntry = 0;
        // Else, if the node is null (no next node), scan is complete.
        } else {
            throw IndexScanCompletedException();
        }
    }
    // Initialize the key to the next entry in the array
    int key = curr->keyArray[nextEntry];
    // See if the entry is valid. If so, increment.
    if (keyOpCodes(lowValInt, lowOp, highValInt, highOp, key) == 1) {
        outRid = curr->ridArray[nextEntry];
        nextEntry++;
    // Invalid entry, then index scan has been completed.
    } else {
        throw IndexScanCompletedException();
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::keyOpCodes
// -----------------------------------------------------------------------------
/**
 * This method checks the key against the passed in operators to determine whether
 * or not the key is valid.
 * @param lowVal    The low value to be tested.
 * @param lowOp     Operation used in testing the low range. (GT and GTE)
 * @param highVal   The high value to be tested.
 * @param highOp    Operation used in testing the high range. (LT and LTE)
 * @param key       The key to check the operations against.
 * @return          Returns 1 if the key lies within the parameters of lowOp
 *                  and highOp, else 1.
 */
const int BTreeIndex::keyOpCodes(int lowVal, const Operator lowOp, int highVal,
                                const Operator highOp, int key) {
    if(lowOp == GT && highOp == LT) {
        if (key < highVal && key > lowVal) {
            return 1;
        } else {
            return 0;
        }
    } else if(lowOp == GTE && highOp == LT) {
        if(key < highVal && key >= lowVal) {
            return 1;
        } else {
            return 0;
        }
    } else if(lowOp == GT && highOp == LTE) {
        if (key <= highVal && key > lowVal) {
            return 1;
        } else {
            return 0;
        }
    } else {
        if(key <= highVal && key >= lowVal) {
            return 1;
        } else {
            return 0;
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
/**
 * This method sets the global scan variable to false so scanning halts, then
 * proceeds to unpin the current page from the buffer pool and sets currentPageNum
 * and nextEntry to non-positive numbers so that they are not falsely added to the
 * tree.
 */
const void BTreeIndex::endScan()
{
    // Throw an error if endScan() has been called when a scan is not in progress.
    if (scanExecuting == false) {
        throw ScanNotInitializedException();
    }
    // End the scan by setting global var to null.
    scanExecuting = false;
    // Free the current page from the buffer pool and set the variable to null.
    bufMgr->unPinPage(file, currentPageNum, false);
    // Free the currentPageData pointer.
    currentPageData = nullptr;
}

}