/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <string>
#include "string.h"
#include <sstream>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"

namespace badgerdb
{

/**
 * @brief Datatype enumeration type.
 */
enum Datatype
{
	INTEGER = 0,
	DOUBLE = 1,
	STRING = 2
};

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator
{
	LT, 	/* Less Than */
	LTE,	/* Less Than or Equal to */
	GTE,	/* Greater Than or Equal to */
	GT		/* Greater Than */
};


/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                                  sibling ptr             key               rid
const  int INTARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     extra pageNo                  key       pageNo
const  int INTARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( PageId ) );

/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair to functions that
 * add to or make changes to the leaf node pages of the tree. Is templated for the key member.
 */
template <class T>
class RIDKeyPair{
public:
	RecordId rid;
	T key;
	void set( RecordId r, T k)
	{
		rid = r;
		key = k;
	}
};

/**
 * @brief Structure to store a key page pair which is used to pass the key and page to functions that make
 * any modifications to the non leaf pages of the tree.
*/
template <class T>
class PageKeyPair{
public:
	PageId pageNo;
	T key;
	void set( int p, T k)
	{
		pageNo = p;
		key = k;
	}
};

/**
 * @brief Overloaded operator to compare the key values of two rid-key pairs
 * and if they are the same compares to see if the first pair has
 * a smaller rid.pageNo value.
*/
template <class T>
bool operator<( const RIDKeyPair<T>& r1, const RIDKeyPair<T>& r2 )
{
	if( r1.key != r2.key )
		return r1.key < r2.key;
	else
		return r1.rid.page_number < r2.rid.page_number;
}

/**
 * @brief The meta page, which holds metadata for Index file, is always first page of the btree index file and is cast
 * to the following structure to store or retrieve information from it.
 * Contains the relation name for which the index is created, the byte offset
 * of the key value on which the index is made, the type of the key and the page no
 * of the root page. Root page starts as page 2 but since a split can occur
 * at the root the root page may get moved up and get a new page no.
*/
struct IndexMetaInfo{
  /**
   * Name of base relation.
   */
	char relationName[20];

  /**
   * Offset of attribute, over which index is built, inside the record stored in pages.
   */
	int attrByteOffset;

  /**
   * Type of the attribute over which index is built.
   */
	Datatype attrType;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
	PageId rootPageNo;
};

/*
Each node is a page, so once we read the page in we just cast the pointer to the page to this struct and use it to access the parts
These structures basically are the format in which the information is stored in the pages for the index file depending on what kind of
node they are. The level memeber of each non leaf structure seen below is set to 1 if the nodes
at this level are just above the leaf nodes. Otherwise set to 0.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
*/
struct NonLeafNodeInt{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYNONLEAFSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ INTARRAYNONLEAFSIZE + 1 ];
};


/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
*/
struct LeafNodeInt{
  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ INTARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};


/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute of a
 * relation. This index supports only one scan at a time.
*/
class BTreeIndex {

 private:

  /**
   * File object for the index file.
   */
	File		*file;

  /**
   * Buffer Manager Instance.
   */
	BufMgr	*bufMgr;

  /**
   * Page number of meta page.
   */
	PageId	headerPageNum;

  /**
   * page number of root page of B+ tree inside index file.
   */
	PageId	rootPageNum;

  /**
   * Datatype of attribute over which index is built.
   */
	Datatype	attributeType;

  /**
   * Offset of attribute, over which index is built, inside records.
   */
	int 		attrByteOffset;

  /**
   * Number of keys in leaf node, depending upon the type of key.
   */
	int			leafOccupancy;

  /**
   * Number of keys in non-leaf node, depending upon the type of key.
   */
	int			nodeOccupancy;


	// MEMBERS SPECIFIC TO SCANNING

  /**
   * True if an index scan has been started.
   */
	bool		scanExecuting;

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */
	int			nextEntry;

  /**
   * Page number of current page being scanned.
   */
	PageId	currentPageNum;

  /**
   * Current Page being scanned.
   */
	Page		*currentPageData;

  /**
   * Low INTEGER value for scan.
   */
	int			lowValInt;

  /**
   * Low DOUBLE value for scan.
   */
	double	lowValDouble;

  /**
   * Low STRING value for scan.
   */
	std::string	lowValString;

  /**
   * High INTEGER value for scan.
   */
	int			highValInt;

  /**
   * High DOUBLE value for scan.
   */
	double	highValDouble;

  /**
   * High STRING value for scan.
   */
	std::string highValString;

  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
	Operator	lowOp;

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
	Operator	highOp;

  /**
   * Used as an extra reference to the root page initialized with the BTree.
   */
	PageId firstRootNum;


 public:

  /**
   * BTreeIndex Constructor.
	 * Check to see if the corresponding index file exists. If so, open the file.
	 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer Manager Instance
   * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
   * @param attrType						Datatype of attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
   */
	BTreeIndex(const std::string & relationName, std::string & outIndexName,
						BufMgr *bufMgrIn,	const int attrByteOffset,	const Datatype attrType);


  /**
   * BTreeIndex Destructor.
	 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
	 * and delete file instance thereby closing the index file.
	 * Destructor should not throw any exceptions. All exceptions should be caught in here itself.
	 * */
	~BTreeIndex();


  /**
	 * Insert a new entry using the pair <value,rid>.
	 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
	 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
	 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
	 * Make sure to unpin pages as soon as you can.
   * @param key			Key to insert, pointer to integer/double/char string
   * @param rid			Record ID of a record whose entry is getting inserted into the index.
	**/
	const void insertEntry(const void* key, const RecordId rid);

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
    const void findSpace(Page* curr, PageId currNum, RIDKeyPair<int> data,
                         int level, PageKeyPair<int>* &child);


    /**
     * This method iterates through a branch node structure and finds the right place
     * to enter a new data node.
     * @param branch    The branch to be added to.
     * @param data      The data to add to said branch node.
     */
    const void addToBranch(NonLeafNodeInt* branch, PageKeyPair<int>* data);


    /**
     * This method iterates through a leaf node structure and finds the right place to
     * insert the data read from the file.
     * @param leaf      The leaf node to be added to.
     * @param data      The data to add to said leaf node.
     */
    const void addToLeaf(LeafNodeInt* leaf, RIDKeyPair<int> data);

    /**
     * This method updates the root of the B-Tree. In this case, the root needs to be split
     * and entries pushed up the tree.
     * @param firstNode     This is the page number of the first entry of the root.
     * @param child         This is the entry that needs to be propogated upwards
     *                      after the split occurs.
     */
    const void newRoot(PageId firstNode, PageKeyPair<int>* child);


    /**
     * This method splits the branch so that the child entry can be entered into the tree.
     * First, a new branch page is allocated memory, then half the array is copied over from
     * the old branch node to the new one. Changes are made to the array to reflect this
     * split.
     * @param child     The entry that will need to be entered after the splitting occurs.
     * @param old       The node that will be split in this function.
     * @param oldNum    PageId that was used to index the node to be split.
     */
    const void branchSplit(PageKeyPair<int>* &child, NonLeafNodeInt* old,
                           PageId oldNum);


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
    const void leafSplit(PageKeyPair<int>* &child, LeafNodeInt* old, PageId oldNum,
                         RIDKeyPair<int> data);

    /**
       * Begin a filtered scan of the index.  For instance, if the method is called
       * using ("a",GT,"d",LTE) then we should seek all entries with a value
       * greater than "a" and less than or equal to "d".
       * If another scan is already executing, that needs to be ended here.
       * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
       * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
     * @param lowVal	Low value of range, pointer to integer / double / char string
     * @param lowOp		Low operator (GT/GTE)
     * @param highVal	High value of range, pointer to integer / double / char string
     * @param highOp	High operator (LT/LTE)
     * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
     * @throws  BadScanrangeException If lowVal > highval
       * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
      **/
	const void startScan(const void* lowVal, const Operator lowOp, const void* highVal, const Operator highOp);


    /**
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
     * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
	**/
	const void scanNext(RecordId& outRid);  // returned record id


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
    const int keyOpCodes(int lowVal, const Operator lowOp, int highVal,
                                     const Operator highOp, int key);

    /**
	 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
	 * @throws ScanNotInitializedException If no scan has been initialized.
	**/
	const void endScan();

};

}