/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2. This set of code shall NOT be redistributed.
 * You should provide implementation for the three algorithms declared in this class.  
 */

package project2;

import project2.Relation.RelationLoader;
import project2.Relation.RelationWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Scanner;

public class Algorithms {

	public static boolean debug = false;

	public Object[] sortSubList(Relation relation, RelationLoader relLoader) {
		ArrayList<Relation> RelationRLst = new ArrayList<Relation>();
		Object[] datas = new Object[2];
		int numIO = 0;
		// Start Relation Loading, Sort, Write Back

		while (relLoader.hasNextBlock()) {
			ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
			Relation tempRel = new Relation(relation.name + RelationRLst.size());
			Block newBR = new Block();

			Block[] blockr = relLoader.loadNextBlocks(Setting.memorySize);
			for (Block br : blockr) {
				if (br != null) {
					numIO++; // increment the IO count from each block loaded
					for (Tuple tr : br.tupleLst) {
						tupleList.add(new Tuple(tr.key, tr.value));
					}
				}
			}

			// Sort and Merge all blocks tuples into a single sublist and
			// distribute all tuples back into blocks of relations
			Collections.sort(tupleList, new Comparator<Tuple>() {
				@Override
				public int compare(Tuple o1, Tuple o2) {
					return Integer.valueOf(o1.key).compareTo(o2.key);
				}
			});

			for (Tuple tr : tupleList) {
				if (newBR.tupleLst.size() < Setting.blockFactor) { // if block
																	// is not
																	// full
					newBR.insertTuple(tr);
				} else { // if block is full
					numIO++; // Writing Blocks back to relation
					tempRel.getRelationWriter().writeBlock(newBR);
					newBR = new Block(); // create new block and add the tuple
											// in
					newBR.insertTuple(tr);
				}
			}
			numIO++; // Writing last Blocks
			tempRel.getRelationWriter().writeBlock(newBR);
			RelationRLst.add(tempRel);
		}
		datas[0] = RelationRLst;
		datas[1] = numIO;
		return datas;

	}

	/**
	 * loading of sorted sub relations in arraylist
	 */
	public Object[] loadSortedRelation(ArrayList<Relation> RelationLst, RelationLoader[] LoaderLst, Block[][] Block,
			Object[] Tuple, int memoryneeded) {
		int numIO = 0;
		Object[] datas = new Object[5];
		boolean error = false;

		// ONLY LOAD the 1st blocks of each sub relation blocks
		// Start Load Relation
		if (RelationLst.size() > memoryneeded) {
			System.out.println("Relation Size: " + RelationLst.size() + ", Memory given: " + memoryneeded);
			System.out.println("\tError - Relation " + RelationLst.get(0).name
					+ " Does not have enought memory : \n\tThe number of total relations sublist is not (M-1)");
			error = true;
		} else {
			for (int r = 0; r < RelationLst.size(); r++) { // load all the sub
															// list relation
															// loaded
				LoaderLst[r] = RelationLst.get(r).getRelationLoader();
			}
			// load 1 block from each sub relation of the original relation
			// where the memory must be big enough to hold.
			for (int r = 0; r < LoaderLst.length; r++) {
				if (LoaderLst[r].hasNextBlock()) {
					Block[r] = LoaderLst[r].loadNextBlocks(1); // At any one
																// time 1 Sorted
																// Relation will
																// only load 1
																// Block out
					if (Block[r][0] != null) {
						ArrayList<Tuple> tempTuple = new ArrayList<Tuple>();
						numIO++; // Read Block to Merge Tuples
						for (int tr = 0; tr < Block[r][0].tupleLst.size(); tr++) {
							tempTuple.add(Block[r][0].tupleLst.get(tr));
						}
						Tuple[r] = tempTuple;
					}
				}
			}
		}
		// End Load Relation (Sorted)
		datas[0] = LoaderLst;
		datas[1] = Block;
		datas[2] = Tuple;
		datas[3] = numIO;
		datas[4] = error;
		return datas;

	}

	/**
	 * get common min values of relations in memory
	 */
	@SuppressWarnings("unchecked")
	public Object[] getCommonValuesLst(ArrayList<Relation> RelationLst, RelationLoader[] LoaderLst, Block[][] Block,
			Object[] Tuple, int minKey) {
		ArrayList<Tuple> holdKeyLst = new ArrayList<Tuple>();
		boolean mayBeInNextBlock = false;
		boolean emptyBlock = false;
		ArrayList<Tuple> tempTupleLst = null;
		Object[] data = new Object[5];
		int numIO = 0;
		// search through all the memory block to find all the same value of the
		// min key
		do {
			mayBeInNextBlock = false;
			for (int t = 0; t < RelationLst.size(); t++) {
				tempTupleLst = (ArrayList<Tuple>) Tuple[t];

				if (tempTupleLst.isEmpty()) {
					emptyBlock = true;
				}
				// check through the blocks to find the smallest key value
				Iterator<Tuple> iT = tempTupleLst.iterator();
				while (iT.hasNext()) {
					Tuple v = iT.next();
					if (v.key == minKey) {
						holdKeyLst.add(v); // adding the tuple into a list for
											// merging process
						if (0 == tempTupleLst.size() - 1) { // check if it is
															// the last item in
															// the block
							mayBeInNextBlock = true; // the item in the next
														// block may be the same
														// item
						}
						iT.remove(); // remove the tuple from the list
					}
					if (tempTupleLst.isEmpty()) { // check if the tuple is empty
						emptyBlock = true;
					}
				}
			}

			if (emptyBlock) { // reloading new block into the memory for
								// re-checking of smallest key value
				for (int t = 0; t < RelationLst.size(); t++) {
					tempTupleLst = (ArrayList<Tuple>) Tuple[t];

					if (tempTupleLst.isEmpty()) {
						if (LoaderLst[t].hasNextBlock()) {
							Block[t] = LoaderLst[t].loadNextBlocks(1); // At any
																		// one
																		// time
																		// 1
																		// Sorted
																		// Relation
																		// will
																		// only
																		// load
																		// 1
																		// Block
																		// out
							if (Block[t][0] != null) {
								ArrayList<Tuple> tempTuple = new ArrayList<Tuple>();
								numIO++; // increment IO count for loading new
											// block into memory
								for (int tr = 0; tr < Block[t][0].tupleLst.size(); tr++) {
									tempTuple.add(Block[t][0].tupleLst.get(tr));
								}
								Tuple[t] = tempTuple;
							}
						}
					}
				}
			}
		} while (mayBeInNextBlock);
		// System.out.println("Relation - " + RelationLst.get(0).name +
		// holdKeyLst);

		data[0] = LoaderLst;
		data[1] = Block;
		data[2] = Tuple;
		data[3] = numIO;
		data[4] = holdKeyLst;
		return data;
	} // get the min value and position of the relation

	@SuppressWarnings("unchecked")
	public int updateMinPos(ArrayList<Relation> RelationLst, Object[] Tuple) {
		int minPos = 0;
		int minKey = 0;
		int min = 0;
		boolean newFlag = true;
		Tuple[] v = new Tuple[RelationLst.size()];
		for (int t = 0; t < RelationLst.size(); t++) {
			ArrayList<Tuple> tempTupleLst = new ArrayList<Tuple>();
			tempTupleLst = (ArrayList<Tuple>) Tuple[t];

			Iterator<Tuple> iT = tempTupleLst.iterator();
			if (iT.hasNext()) {
				v[t] = iT.next();

				if (newFlag) {
					newFlag = false;
					minPos = t;
					minKey = v[t].key;
					continue;
				}
				if (v[t] != null && v[minPos] != null) {
					if (v[t].key < v[minPos].key) {
						minPos = t; // which Relation to choose from
						minKey = v[t].key;
					}
				}
			}
		}
		min = minKey;
		return min;
	}

	/**
	 * Sort the relation using Setting.memorySize buffers of memory
	 * 
	 * @param rel
	 *            is the relation to be sorted.
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	@SuppressWarnings("unchecked")
	public int mergeSortRelation(Relation rel) {

		int numIO = 0;

		// Insert your code here!

		int memForMerge = Setting.memorySize - 1; // 1 memory slot of output of
													// sort merge result;

		if (debug) {
			System.out.println("-- Start Merge Sort Relation --");
		}

		int minKey = -1;
		int minKeyPos = -1;

		/* Initialize Blocks for input/output buffer and loading */
		Block[] inputBuf = new Block[memForMerge];
		Block outputB = new Block();
		Block[] blocks; // Temp

		/* Initialize RelationWriter */
		Relation outputRel = new Relation("OutputRel");
		RelationWriter rWriter = outputRel.getRelationWriter();

		/* Create an arraylist of Relation to store the sorted sublist of rel */
		ArrayList<Relation> relList = new ArrayList<Relation>();

		RelationLoader relLoader = rel.getRelationLoader();

		if (debug) {
			System.out.println("-- Start Sublists sorting --");
		}

		Object[] sortSub = sortSubList(rel, relLoader);

		relList = (ArrayList<Relation>) sortSub[0];
		numIO = (int) sortSub[1];

		if (debug) {
			System.out.println("numIO: [after sorting] " + numIO);
			System.out.println("\nNumber of sublist:" + relList.size() + "\n");
			System.out.println("memoryForUse:" + memForMerge + "\n");
			System.out.println("-- End Sublists sorting --");
		}

		/* Minimum requirement where B(rel)/memorySize <= memForMerge */
		/* B(rel)/memorySize is the number of sorted sublist */
		if (relList.size() <= memForMerge) {
			/* create loader for each relation sublist */
			ArrayList<RelationLoader> relLoaderList = new ArrayList<RelationLoader>();
			for (int i = 0; i < relList.size(); i++) {
				RelationLoader relationLoader = relList.get(i).getRelationLoader();
				relLoaderList.add(relationLoader);
			}
			if (debug) {
				System.out.println("relLoaderList RELOAD: " + relLoaderList.size());
			}

			/* Load first block of each sorted sublist into input buffer */
			for (int i = 0; i < relLoaderList.size(); i++) {
				if (relLoaderList.get(i).hasNextBlock()) {
					blocks = relLoaderList.get(i).loadNextBlocks(1);
					inputBuf[i] = blocks[0];
					numIO = numIO + 1;
				}
			}
			// If all 3 sublist is empty then terminate.
			boolean merged = false;
			do {
				minKey = -1;
				minKeyPos = -1;
				/* Compare if not empty */
				for (int i = 0; i < inputBuf.length; i++) {
					if (inputBuf[i] != null) {
						// if empty, Refill
						if (inputBuf[i].tupleLst.isEmpty()) {
							// Reload from sublist
							if (relLoaderList.get(i).hasNextBlock()) {
								blocks = relLoaderList.get(i).loadNextBlocks(1); //
								inputBuf[i] = blocks[0]; //
								numIO = numIO + 1;
							}

						}
						// If not empty, compare
						if (!inputBuf[i].tupleLst.isEmpty()) {
							/* Initialization of value */
							if (minKey == -1) {
								minKey = inputBuf[i].tupleLst.get(0).key;
								minKeyPos = i;
							} else {
								/*
								 * compare key - if buf value smaller, replace
								 */
								if (minKey >= inputBuf[i].tupleLst.get(0).key) {
									minKey = inputBuf[i].tupleLst.get(0).key;
									minKeyPos = i;
								}
							}
						}
					}
				}

				// Transfer minKey to output buffer and remove
				if (!inputBuf[minKeyPos].tupleLst.isEmpty()) {
					if (!outputB.insertTuple(inputBuf[minKeyPos].tupleLst.get(0))) {
						rWriter.writeBlock(outputB);
						// numIO = numIO + 1;
						outputB = new Block();
						outputB.insertTuple(inputBuf[minKeyPos].tupleLst.get(0));
						if (debug) {
							System.out.println("numIO: [after flush] " + numIO);
						}
					}
					/* Remove tuple that has been input to output buffer */
					inputBuf[minKeyPos].tupleLst.remove(0);
				}

				// Test if sublist is empty
				merged = false;
				for (int j = 0; j < relLoaderList.size(); j++) {
					if (relLoaderList.get(j).hasNextBlock()) {
						merged = true;
						break;
					}
				}
				if (merged == false) {
					// Check if memory buffer has residual values
					for (int j = 0; j < relLoaderList.size(); j++) {
						if (inputBuf[j].getNumTuples() != 0) {
							merged = true;
							break;
						}
					}
				}
			} while (merged);

			if (outputB.getNumTuples() != 0) {
				rWriter.writeBlock(outputB);
				if (debug) {
					System.out.println("numIO: [after final flush] " + numIO);
				}
			}
			//
			if (debug) {
				outputRel.printRelation(true, true);
				System.out.println("-- End Merge Sort Relation --");
				System.out.println("Final numIO: " + numIO);
				System.out.println("Num tuples: " + outputRel.getNumTuples());
			}
		} else {
			numIO = -1;
			System.out.println("ERROR: Merge Sort terminated.");
			System.out.println("Number of sublist exceeds amount of memory buffers available");
		}

		return numIO;
	}

	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory
	 * to produce the result relation relRS
	 * 
	 * @param relR
	 *            is one of the relation in the join
	 * @param relS
	 *            is the other relation in the join
	 * @param relRS
	 *            is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int hashJoinRelations(Relation relR, Relation relS, Relation relRS) {
		int numIO = 0;

		// Insert your code here!

		int M = Setting.memorySize;
		if (M < 2) {
			System.out.println("Fail to meet minimum memory size requirement");
			return 0;
		}

		// declare buffers: array of blocks
		Block buffers[] = new Block[M];
		for (int i = 0; i < M; i++) // initialise buffer
		{
			buffers[i] = new Block();
		}
		// partition S
		// declare buckets
		@SuppressWarnings("unchecked")
		ArrayList<Block>[] bucketsS = new ArrayList[M - 1];
		for (int i = 0; i < bucketsS.length; i++) {
			bucketsS[i] = new ArrayList<Block>();
		}
		RelationLoader relLoader = relS.getRelationLoader();
		while (relLoader.hasNextBlock()) { // read the input into the last
											// buffer
			buffers[M - 1] = relLoader.loadNextBlocks(1)[0];
			numIO++;

			for (Tuple t : buffers[M - 1].tupleLst) {
				int tupleKey = t.key;
				// hash function mod(M-1)
				int outBuffer = tupleKey % (M - 1);

				if (outBuffer > M - 1) {
					System.out.println("Partition error: hash function result > M-1");
					return 0;
				}
				if (!buffers[outBuffer].insertTuple(t)) {
					// add block in buffers[i] to buckets[i]
					bucketsS[outBuffer].add(buffers[outBuffer]);
					numIO++;
					// initialise a new empty block
					buffers[outBuffer] = new Block();
					buffers[outBuffer].insertTuple(t);
				}
			}
		}
		for (int i = 0; i < M - 1; i++) {
			if (buffers[i].getNumTuples() != 0) // buffer is not empty
			{// write buffer to disk
				bucketsS[i].add(buffers[i]);
				numIO++;
			}
		}

		// partition R
		for (int i = 0; i < M; i++)// initialise buffer
		{
			buffers[i] = new Block();
		}
		@SuppressWarnings("unchecked")
		ArrayList<Block>[] bucketsR = new ArrayList[M - 1];
		for (int i = 0; i < bucketsR.length; i++) {
			bucketsR[i] = new ArrayList<Block>();
		}

		relLoader = relR.getRelationLoader();
		while (relLoader.hasNextBlock()) { // read the input into the last
											// buffer
			buffers[M - 1] = relLoader.loadNextBlocks(1)[0];
			numIO++;

			for (Tuple t : buffers[M - 1].tupleLst) {
				int tupleKey = t.key;
				// hash function mod(M-1)
				int outBuffer = tupleKey % (M - 1);

				if (outBuffer > M - 1) {
					System.out.println("Partition error: hash function result > M-1");
					return 0;
				}
				if (!buffers[outBuffer].insertTuple(t)) {
					// add block in buffers[i] to buckets[i]
					bucketsR[outBuffer].add(buffers[outBuffer]);
					numIO++;
					// initialise a new empty block
					buffers[outBuffer] = new Block();
					buffers[outBuffer].insertTuple(t);
				}
			}
		}
		for (int i = 0; i < M - 1; i++) {
			if (buffers[i].getNumTuples() != 0) // buffer is not empty
			{// write buffer to disk
				bucketsR[i].add(buffers[i]);
				numIO++;
			}
		}

		// join
		RelationWriter sWriter = relRS.getRelationWriter();

		for (int i = 0; i < M; i++)// initialise buffer
		{
			buffers[i] = new Block();
		}
		Block tempBlock = new Block();

		for (int bucket = 0; bucket < M - 1; bucket++) {
			int bucketSize = bucketsS[bucket].size();
			if (bucketSize > M - 1) // if number of block > M-1:return error
			{
				System.out.println(
						bucket + "th bucket has size: " + bucketSize + " ,larger than memory buffer size " + (M - 1));
				return 0;
			}
			for (int block = 0; block < bucketSize; block++) {// save one S
																// bucket to mem
																// buffer
				buffers[block] = bucketsS[bucket].get(block);
				numIO++;
			}
			for (int block = 0; block < bucketsR[bucket].size(); block++) {// save
																			// one
																			// R
																			// block
																			// into
																			// buffers[M-1]
				buffers[M - 1] = bucketsR[bucket].get(block);
				numIO++;
				for (int i = 0; i < M - 1; i++) {
					for (int rTuple = 0; rTuple < buffers[M - 1].getNumTuples(); rTuple++) {
						for (int sTuple = 0; sTuple < buffers[i].getNumTuples(); sTuple++) {// compare
																							// tuple
																							// in
																							// R
																							// block
																							// &
																							// tuple
																							// in
																							// ith
																							// S
																							// block
							Tuple r = buffers[M - 1].tupleLst.get(rTuple);
							Tuple s = buffers[i].tupleLst.get(sTuple);
							if (r.key == s.key) {// join the tuples
								JointTuple joinTuple = new JointTuple(r, s);
								if (!tempBlock.insertTuple(joinTuple)) {// write
																		// to
																		// relRS
																		// if
																		// tempBlock
																		// is
																		// full
									sWriter.writeBlock(tempBlock);
									// initialise new block
									tempBlock = new Block();
									tempBlock.insertTuple(joinTuple);
								}
							}
						}
					}
				}
			}
		}

		if (tempBlock.getNumTuples() != 0) // temp block is not empty
		{// write temp block to relRS
			sWriter.writeBlock(tempBlock);
		}

		return numIO;
	}

	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory
	 * to produce the result relation relRS
	 * 
	 * @param relR
	 *            is one of the relation in the join
	 * @param relS
	 *            is the other relation in the join
	 * @param relRS
	 *            is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */

	@SuppressWarnings("unchecked")
	public int refinedSortMergeJoinRelations(Relation relR, Relation relS, Relation relRS) {
		int numIO = 0;

		// Insert your code here!

		int numIOR = 0;
		int numIOS = 0;

		ArrayList<Relation> RelationRLst = new ArrayList<Relation>(); // A
																		// arraylist
																		// of
																		// output
																		// relations
																		// for R
		RelationLoader rLoader = relR.getRelationLoader();
		ArrayList<Relation> RelationSLst = new ArrayList<Relation>(); // A
																		// arraylist
																		// of
																		// output
																		// relations
																		// for S
		RelationLoader sLoader = relS.getRelationLoader();

		if ((relR.getNumBlocks() + relS.getNumBlocks()) <= (Setting.memorySize * Setting.memorySize) - 1) {
			// Load, Sort Sublist, Write back - 2(B(R) + B(S))
			// Pass in relation R to be sorted and split in to multiple sublist
			Object[] relRData = sortSubList(relR, rLoader);
			RelationRLst = (ArrayList<Relation>) relRData[0];
			numIOR += (int) relRData[1];

			// Pass in relation S to be sorted and split in to multiple sublist
			Object[] relSData = sortSubList(relS, sLoader);
			RelationSLst = (ArrayList<Relation>) relSData[0];
			numIOS += (int) relSData[1];

			// compute the minimum required memory for R
			int memNeededR = (relR.getNumBlocks() / Setting.memorySize);
			int tempMemr = relR.getNumBlocks() % Setting.memorySize;
			if (tempMemr != 0) {
				memNeededR++;
			}

			// compute the minimum required memory for S
			int memNeededS = (relS.getNumBlocks() / Setting.memorySize);
			int tempMems = relS.getNumBlocks() % Setting.memorySize;
			if (tempMems != 0) {
				memNeededS++;
			}

			// Allocate the memory to the relation
			int mem = (Setting.memorySize - 1);
			int memS = mem - memNeededR;
			int memR = mem - memS;

			System.out.println("Buffer: Memory Allocated-1");
			System.out.println("Relation R: Tuples-" + relR.getNumTuples() + ", Blocks-" + relR.getNumBlocks()
					+ ", Number of Sub List-" + RelationRLst.size() + ", Memory Needed-" + memNeededR
					+ ", Memory Allocated-" + memR);
			System.out.println("Relation S: Tuples-" + relS.getNumTuples() + ", Blocks-" + relS.getNumBlocks()
					+ ", Number of Sub List-" + RelationSLst.size() + ", Memory Needed-" + memNeededS
					+ ", Memory Allocated-" + memS);
			// Start Load Relation R (Sorted)
			RelationLoader[] rRLoaderLst = new RelationLoader[RelationRLst.size()];
			Block[][] rRBlock = new Block[RelationRLst.size()][1];
			Object[] rRTuple = new Object[RelationRLst.size()]; // Array of
																// Arraylist
																// (Tuple)

			// Load the relation R sorted sublist, only 1 block from each sorted
			// sublist
			// Computing min memory required for R
			Object[] sortedRelRData = loadSortedRelation(RelationRLst, rRLoaderLst, rRBlock, rRTuple, memR);
			rRLoaderLst = (RelationLoader[]) sortedRelRData[0];
			if ((boolean) sortedRelRData[4] == true) {
				return 0;
			}
			rRBlock = (Block[][]) sortedRelRData[1];
			rRTuple = (Object[]) sortedRelRData[2];
			numIOR += (int) sortedRelRData[3];

			// Start Load Relation S (Sorted)
			RelationLoader[] rSLoaderLst = new RelationLoader[RelationSLst.size()];
			Block[][] rSBlock = new Block[RelationSLst.size()][1];
			Object[] rSTuple = new Object[RelationSLst.size()]; // Array of
																// Arraylist
																// (Tuple)

			// Load the relation S sorted sublist, only 1 block from each sorted
			// sublist
			Object[] sortedRelSData = loadSortedRelation(RelationSLst, rSLoaderLst, rSBlock, rSTuple, memS);
			rSLoaderLst = (RelationLoader[]) sortedRelSData[0];
			if ((boolean) sortedRelSData[4] == true) {
				return 0;
			}
			rSBlock = (Block[][]) sortedRelSData[1];
			rSTuple = (Object[]) sortedRelSData[2];
			numIOS += (int) sortedRelSData[3];

			// Merge Relation
			Block newBRS = new Block(); // output buffer

			int minR = 0;
			int minS = 0;
			Object[] dataR = null;
			Object[] dataS = null;
			ArrayList<Tuple> holdRKeyLst = null;
			ArrayList<Tuple> holdSKeyLst = null;
			boolean merge = false;

			// Merging Process
			// load all the information needed for relation R
			minR = updateMinPos(RelationRLst, rRTuple); // Finding min key value
			dataR = getCommonValuesLst(RelationRLst, rRLoaderLst, rRBlock, rRTuple, minR);
			rRLoaderLst = (RelationLoader[]) dataR[0];
			rRBlock = (Block[][]) dataR[1];
			rRTuple = (Object[]) dataR[2];
			numIOR += (int) dataR[3];
			holdRKeyLst = (ArrayList<Tuple>) dataR[4];

			// load all the information needed for relation S
			minS = updateMinPos(RelationSLst, rSTuple); // Finding min key value
			dataS = getCommonValuesLst(RelationSLst, rSLoaderLst, rSBlock, rSTuple, minS);
			rSLoaderLst = (RelationLoader[]) dataS[0];
			rSBlock = (Block[][]) dataS[1];
			rSTuple = (Object[]) dataS[2];
			numIOS += (int) dataS[3];
			holdSKeyLst = (ArrayList<Tuple>) dataS[4];

			do {
				if (minR == minS) { // if both minimum key is found than perform
									// merging
					merge = true;
					for (int r = 0; r < holdRKeyLst.size(); r++) {
						for (int s = 0; s < holdSKeyLst.size(); s++) {
							Tuple newTRS = new JointTuple(holdRKeyLst.get(r), holdSKeyLst.get(s));
							// if the block space is still avaliable insert it
							// in
							if (newBRS.tupleLst.size() < Setting.blockFactor) {
								newBRS.insertTuple(newTRS);
							} else { // else, write the block to output relation
										// and create a new block for the tuple
										// to be inserted
								relRS.getRelationWriter().writeBlock(newBRS);
								newBRS = new Block();
								newBRS.insertTuple(newTRS);
							}
						}
					}
				}

				if (minR > minS) {
					// load all the information needed for relation S
					minS = updateMinPos(RelationSLst, rSTuple); // Finding min
																// key value
					dataS = getCommonValuesLst(RelationSLst, rSLoaderLst, rSBlock, rSTuple, minS);
					rSLoaderLst = (RelationLoader[]) dataS[0];
					rSBlock = (Block[][]) dataS[1];
					rSTuple = (Object[]) dataS[2];
					numIOS += (int) dataS[3];
					holdSKeyLst = (ArrayList<Tuple>) dataS[4];
				} else if (minR < minS) {
					minR = updateMinPos(RelationRLst, rRTuple); // Finding min
																// key value
					dataR = getCommonValuesLst(RelationRLst, rRLoaderLst, rRBlock, rRTuple, minR);
					rRLoaderLst = (RelationLoader[]) dataR[0];
					rRBlock = (Block[][]) dataR[1];
					rRTuple = (Object[]) dataR[2];
					numIOR += (int) dataR[3];
					holdRKeyLst = (ArrayList<Tuple>) dataR[4];
				}

				// if there is a merge operation, Get both latest min key list
				if (merge) {
					merge = false;
					// load all the information needed for relation R
					minR = updateMinPos(RelationRLst, rRTuple); // Finding min
																// key value
					dataR = getCommonValuesLst(RelationRLst, rRLoaderLst, rRBlock, rRTuple, minR);
					rRLoaderLst = (RelationLoader[]) dataR[0];
					rRBlock = (Block[][]) dataR[1];
					rRTuple = (Object[]) dataR[2];
					numIOR += (int) dataR[3];
					holdRKeyLst = (ArrayList<Tuple>) dataR[4];

					// load all the information needed for relation S
					minS = updateMinPos(RelationSLst, rSTuple); // Finding min
																// key value
					dataS = getCommonValuesLst(RelationSLst, rSLoaderLst, rSBlock, rSTuple, minS);
					rSLoaderLst = (RelationLoader[]) dataS[0];
					rSBlock = (Block[][]) dataS[1];
					rSTuple = (Object[]) dataS[2];
					numIOS += (int) dataS[3];
					holdSKeyLst = (ArrayList<Tuple>) dataS[4];

				}
			} while (!holdRKeyLst.isEmpty() && !holdSKeyLst.isEmpty());
			relRS.getRelationWriter().writeBlock(newBRS);
		} else {
			System.out
					.println("Error - Memory not enough, unable to perfrom Refind sort merge join \n- B(R)+B(S) > M*M :"
							+ relR.getNumBlocks() + " + " + relS.getNumBlocks() + " > "
							+ (Setting.memorySize * Setting.memorySize));
		}
		numIO = numIOR + numIOS;

		return numIO;
	}

	public void testMerge(int cases, String testType) {
		int numIO;
		int numTuples;
		Relation relR;

		System.out.println("---------Populating relation----------");
		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains " + numTuples + " tuples.");
		System.out.println("---------Finish populating relations----------\n");
		System.out.println("TestCase " + cases + ": " + testType);
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R)) = 3(" + relR.getNumBlocks() + ") = " + "(Expected IO Cost)");
		System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + "/" + Setting.memorySize + " <= "
				+ (Setting.memorySize * Setting.memorySize));

		numIO = mergeSortRelation(relR);
		if (numIO >= 0) {
			System.out.println("\nMerge Sort Actual IO Cost : " + numIO);
			relR.printRelation(false, false);
		
		}
		System.out.println("\n");
	}

	/**
	 * Testing cases Merge.
	 */
	public static void testCasesMerge() {

		Algorithms algorithm = new Algorithms();

		// Test Case 1
		Setting.blockFactor = 20;
		Setting.memorySize = 20;
		algorithm.testMerge(1, "Minimum Memory and Block Factor");

		// Test Case 2
		Setting.blockFactor = 30;
		Setting.memorySize = 20;
		algorithm.testMerge(2, "Minimum Memory and Block Factor");

		// Test Case 3
		Setting.blockFactor = 40;
		Setting.memorySize = 20;
		algorithm.testMerge(3, "Minimum Memory and Block Factor");

		// Test Case 4
		Setting.blockFactor = 50;
		Setting.memorySize = 20;
		algorithm.testMerge(4, "Minimum Memory and Block Factor");
		
		// Test Case 5
		Setting.blockFactor = 60;
		Setting.memorySize = 20;
		algorithm.testMerge(5, "Minimum Memory and Block Factor");
		
		// Test Case 6
		Setting.blockFactor = 20;
		Setting.memorySize = 30;
		algorithm.testMerge(6, "Minimum Memory and Block Factor");
		
		// Test Case 7
		Setting.blockFactor = 20;
		Setting.memorySize = 40;
		algorithm.testMerge(7, "Minimum Memory and Block Factor");
		
		// Test Case 8
		Setting.blockFactor = 20;
		Setting.memorySize = 50;
		algorithm.testMerge(8, "Minimum Memory and Block Factor");
	
		// Test Case 9
		Setting.blockFactor = 20;
		Setting.memorySize = 60;
		algorithm.testMerge(9, "Minimum Memory and Block Factor");
	

	}

	/**
	 * Testing cases RefineMerge.
	 */
	public static void testCasesRefinedMerge() {
		int refineIO;
		int numTuples;
		Relation relR;
		Relation relS;
		Relation relRS;

		Algorithms algorithm = new Algorithms();
		// Main Test Case 1
		// Test Case 1
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		Setting.memorySize = 20;

		System.out.println("---------Populating two relations----------");
		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains " + numTuples + " tuples.");

		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains " + numTuples + " tuples.");

		relRS = new Relation("RelRS");
		System.out.println("---------Finish populating relations----------\n");

		// ----------------------------------------------------------------------------------------------
		// Minimum required is 3 memory as 2 for reading 1 for buffer, as memory
		// decreases the block factor have to increase, the best case is to load
		// the whole relation into the memory
		System.out.println("Test Case 1.1:");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");
		System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + " + " + relS.getNumBlocks() + " = ("
				+ (relR.getNumBlocks() + relS.getNumBlocks()) + ")" + " <= "
				+ (Setting.memorySize * Setting.memorySize));

		refineIO = algorithm.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("\nRefined Sort Merge Join Actural IO Cost : " + refineIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		// End of Test Case 1

		System.out.println("\n");

		// Test Case 2
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 30;
		Setting.memorySize = 20;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		// ----------------------------------------------------------------------------------------------
		System.out.println("Test Case 1.2:");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");
		System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + " + " + relS.getNumBlocks() + " = ("
				+ (relR.getNumBlocks() + relS.getNumBlocks()) + ")" + " <= "
				+ (Setting.memorySize * Setting.memorySize));

		refineIO = algorithm.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("\nRefined Sort Merge Join Actural IO Cost : " + refineIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		// End of Test Case 2

		System.out.println("\n");

		// Test Case 3
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 40;
		Setting.memorySize = 20;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		// ----------------------------------------------------------------------------------------------
		System.out.println("Test Case 1.3:");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");
		System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + " + " + relS.getNumBlocks() + " = ("
				+ (relR.getNumBlocks() + relS.getNumBlocks()) + ")" + " <= "
				+ (Setting.memorySize * Setting.memorySize));

		refineIO = algorithm.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("\nRefined Sort Merge Join Actural IO Cost : " + refineIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		// End of Test Case 3

		System.out.println("\n");

		// Test Case 4
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 50;
		Setting.memorySize = 20;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		// ----------------------------------------------------------------------------------------------
		System.out.println("Test Case 1.4:");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");
		System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + " + " + relS.getNumBlocks() + " = ("
				+ (relR.getNumBlocks() + relS.getNumBlocks()) + ")" + " <= "
				+ (Setting.memorySize * Setting.memorySize));

		refineIO = algorithm.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("\nRefined Sort Merge Join Actural IO Cost : " + refineIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		// End of Test Case 4

		System.out.println("\n");

		// Test Case 5
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 60;
		Setting.memorySize = 20;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		// ----------------------------------------------------------------------------------------------
		System.out.println("Test Case 1.5:");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");
		System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + " + " + relS.getNumBlocks() + " = ("
				+ (relR.getNumBlocks() + relS.getNumBlocks()) + ")" + " <= "
				+ (Setting.memorySize * Setting.memorySize));

		refineIO = algorithm.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("\nRefined Sort Merge Join Actural IO Cost : " + refineIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		// End of Test Case 5

		System.out.println("\n");

		// Test Case 6
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		Setting.memorySize = 30;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		// ----------------------------------------------------------------------------------------------
		System.out.println("Test Case 1.6:");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");
		System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + " + " + relS.getNumBlocks() + " = ("
				+ (relR.getNumBlocks() + relS.getNumBlocks()) + ")" + " <= "
				+ (Setting.memorySize * Setting.memorySize));

		refineIO = algorithm.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("\nRefined Sort Merge Join Actural IO Cost : " + refineIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		// End of Test Case 6

		System.out.println("\n");

		// Test Case 7
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		Setting.memorySize = 40;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		// ----------------------------------------------------------------------------------------------
		System.out.println("Test Case 1.7:");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");
		System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + " + " + relS.getNumBlocks() + " = ("
				+ (relR.getNumBlocks() + relS.getNumBlocks()) + ")" + " <= "
				+ (Setting.memorySize * Setting.memorySize));

		refineIO = algorithm.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("\nRefined Sort Merge Join Actural IO Cost : " + refineIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		// End of Test Case 7

		System.out.println("\n");

		// Test Case 8
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		Setting.memorySize = 50;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		// ----------------------------------------------------------------------------------------------
		System.out.println("Test Case 1.8:");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");
		System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + " + " + relS.getNumBlocks() + " = ("
				+ (relR.getNumBlocks() + relS.getNumBlocks()) + ")" + " <= "
				+ (Setting.memorySize * Setting.memorySize));

		refineIO = algorithm.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("\nRefined Sort Merge Join Actural IO Cost : " + refineIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		// End of Test Case 8

		System.out.println("\n");

		// Test Case 9
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		Setting.memorySize = 60;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		// ----------------------------------------------------------------------------------------------
		System.out.println("Test Case 1.9:");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");
		System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + " + " + relS.getNumBlocks() + " = ("
				+ (relR.getNumBlocks() + relS.getNumBlocks()) + ")" + " <= "
				+ (Setting.memorySize * Setting.memorySize));

		refineIO = algorithm.refinedSortMergeJoinRelations(relR, relS, relRS);
		System.out.println("\nRefined Sort Merge Join Actural IO Cost : " + refineIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		// End of Test Case 9

		System.out.println("\n");

		// Main Test Case 2
		// ----------------------------------------------------------------------------------------------
		// Minimum required is 3 memory as 2 for reading 1 for buffer, as memory
		// decreases the block factor have to increase, the best case is to load
		// the whole relation into the memory
		// Test Case 1
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		int j = 1;
		for (int i = 20; i > 0; i--) {
			Setting.memorySize = i;

			relR = new Relation("RelR");
			numTuples = relR.populateRelationFromFile("RelR.txt");
			relS = new Relation("RelS");
			numTuples = relS.populateRelationFromFile("RelS.txt");
			relRS = new Relation("RelRS");

			// ----------------------------------------------------------------------------------------------
			System.out.println("Test Case 2." + j + ":");
			System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
			System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
					+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");
			System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + " + " + relS.getNumBlocks()
					+ " = (" + (relR.getNumBlocks() + relS.getNumBlocks()) + ")" + " <= "
					+ (Setting.memorySize * Setting.memorySize));

			refineIO = algorithm.refinedSortMergeJoinRelations(relR, relS, relRS);
			System.out.println("\nRefined Sort Merge Join Actural IO Cost : " + refineIO);
			System.out.print("\t");
			relRS.printRelation(false, false);

			System.out.println("\n");
			j++;
		}

		// Main Test Case 3
		// ----------------------------------------------------------------------------------------------
		// Minimum required is 3 memory as 2 for reading 1 for buffer, as memory
		// decreases the block factor have to increase, the best case is to load
		// the whole relation into the memory
		// Test Case 1
		// ----------------------------------------------------------------------------------
		Setting.memorySize = 20;
		int k = 1;
		for (int i = 10; i > 0; i--) {
			Setting.blockFactor = i;

			relR = new Relation("RelR");
			numTuples = relR.populateRelationFromFile("RelR.txt");
			relS = new Relation("RelS");
			numTuples = relS.populateRelationFromFile("RelS.txt");
			relRS = new Relation("RelRS");

			// ----------------------------------------------------------------------------------------------
			System.out.println("Test Case 3." + k + ":");
			System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
			System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
					+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");
			System.out.println("Minimum Memory condition = " + relR.getNumBlocks() + " + " + relS.getNumBlocks()
					+ " = (" + (relR.getNumBlocks() + relS.getNumBlocks()) + ")" + " <= "
					+ (Setting.memorySize * Setting.memorySize));

			refineIO = algorithm.refinedSortMergeJoinRelations(relR, relS, relRS);
			System.out.println("\nRefined Sort Merge Join Actural IO Cost : " + refineIO);
			System.out.print("\t");
			relRS.printRelation(false, false);
			System.out.println("\n");
			k++;
		}

	}

	/**
	 * Testing cases Hashing.
	 */
	public static void testCasesHashing() {
		int hashIO;
		int numTuples;
		Relation relR;
		Relation relS;
		Relation relRS;

		Algorithms algorithm = new Algorithms();

		// Test Case 1 Â - Finding the minimum Block factor for the given memory
		// case 1.1
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		Setting.memorySize = 20;

		System.out.println("---------Populating two relations----------");
		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains " + numTuples + " tuples.");

		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains " + numTuples + " tuples.");

		relRS = new Relation("RelRS");
		System.out.println("---------Finish populating relations----------\n");

		System.out.println("Test Case 1.1: ");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");

		hashIO = algorithm.hashJoinRelations(relR, relS, relRS);
		System.out.println("\nHash Join Actual IO Cost : " + hashIO);
		System.out.print("\t");
		relRS.printRelation(false, false);

		System.out.println("\n");

		// Test Case 1.2
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 30;
		Setting.memorySize = 20;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		System.out.println("Test Case 1.2: ");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");

		hashIO = algorithm.hashJoinRelations(relR, relS, relRS);
		System.out.println("\nHash Join Actual IO Cost : " + hashIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		System.out.println("\n");

		// Test Case 1.3
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 40;
		Setting.memorySize = 20;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		System.out.println("Test Case 1.3: ");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");

		hashIO = algorithm.hashJoinRelations(relR, relS, relRS);
		System.out.println("\nHash Join Actual IO Cost : " + hashIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		System.out.println("\n");

		// Test Case 1.4
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 50;
		Setting.memorySize = 20;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		System.out.println("Test Case 1.4: ");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");

		hashIO = algorithm.hashJoinRelations(relR, relS, relRS);
		System.out.println("\nHash Join Actual IO Cost : " + hashIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		System.out.println("\n");

		// Test Case 1.5
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 60;
		Setting.memorySize = 20;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		System.out.println("Test Case 1.5: ");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");

		hashIO = algorithm.hashJoinRelations(relR, relS, relRS);
		System.out.println("\nHash Join Actual IO Cost : " + hashIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		System.out.println("\n");

		// Test Case 1.6
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		Setting.memorySize = 30;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		System.out.println("Test Case 1.6:=");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");

		hashIO = algorithm.hashJoinRelations(relR, relS, relRS);
		System.out.println("\nHash Join Actual IO Cost : " + hashIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		System.out.println("\n");

		// Test Case 1.7
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		Setting.memorySize = 40;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		System.out.println("Test Case 1.7: ");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");

		hashIO = algorithm.hashJoinRelations(relR, relS, relRS);
		System.out.println("\nHash Join Actual IO Cost : " + hashIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		System.out.println("\n");

		// Test Case 1.8
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		Setting.memorySize = 50;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		System.out.println("Test Case 1.8: ");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");

		hashIO = algorithm.hashJoinRelations(relR, relS, relRS);
		System.out.println("\nHash Join Actual IO Cost : " + hashIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		System.out.println("\n");

		// Test Case 1.9
		// ----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		Setting.memorySize = 60;

		relR = new Relation("RelR");
		numTuples = relR.populateRelationFromFile("RelR.txt");
		relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		relRS = new Relation("RelRS");

		System.out.println("Test Case 1.9: ");
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
				+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");

		hashIO = algorithm.hashJoinRelations(relR, relS, relRS);
		System.out.println("\nHash Join Actual IO Cost : " + hashIO);
		System.out.print("\t");
		relRS.printRelation(false, false);
		System.out.println("\n");

		// Test Case 2 - Finding the minimum memory based on given block
		// factor----------------------------------------------------------------------------------
		Setting.blockFactor = 20;
		int k = 1;
		for (int i = 20; i > 0; i--) {
			Setting.memorySize = i;

			relR = new Relation("RelR");
			numTuples = relR.populateRelationFromFile("RelR.txt");
			relS = new Relation("RelS");
			numTuples = relS.populateRelationFromFile("RelS.txt");
			relRS = new Relation("RelRS");

			System.out.println("Test Case 2." + k + ":");
			System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
			System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
					+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");

			hashIO = algorithm.hashJoinRelations(relR, relS, relRS);
			System.out.println("\nHash Join Actual IO Cost : " + hashIO);
			System.out.print("\t");
			relRS.printRelation(false, false);
			System.out.println("\n");
			k++;
		}

		// Test Case 3  - Finding the minimum block factor based on given memory
		// size----------------------------------------------------------------------------------
		Setting.memorySize = 20;
		int j = 1;
		for (int i = 10; i > 0; i--) {
			Setting.blockFactor = i;

			relR = new Relation("RelR");
			numTuples = relR.populateRelationFromFile("RelR.txt");
			relS = new Relation("RelS");
			numTuples = relS.populateRelationFromFile("RelS.txt");
			relRS = new Relation("RelRS");

			System.out.println("Test Case 3." + j + ":");
			System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
			System.out.println("3(B(R) + B(S)) = " + "3(" + relR.getNumBlocks() + " + " + relS.getNumBlocks() + ") = "
					+ 3 * (relR.getNumBlocks() + relS.getNumBlocks()) + "(Expected IO Cost)");

			hashIO = algorithm.hashJoinRelations(relR, relS, relRS);
			System.out.println("\nHash Join Actual IO Cost : " + hashIO);
			System.out.print("\t");
			relRS.printRelation(false, false);
			System.out.println("\n");
			j++;
		}

	}

	/**
	 * Testing cases.
	 */
	public static void testCases(int option) {

		// Insert your test cases here!

		switch (option) {
		case 1:
			Algorithms.testCasesMerge();
			break;
		case 2:
			Algorithms.testCasesRefinedMerge();
			break;
		case 3:
			Algorithms.testCasesHashing();
			break;
		case 4:
			System.out.println("Exiting...");
			System.exit(0);
			break;
		}

	}

	/**
	 * This main method provided for testing purpose
	 * 
	 * @param arg
	 */
	public static void main(String[] arg) {
		// put debug = true for printing out raw results for option 1, merge sort
		
		
		debug = true;
		Scanner in = new Scanner(System.in);
		do {
			System.out.println("Algorithms Menu");
			System.out.println("------------------------------");
			System.out.println("1 - Merge Sort ");
			System.out.println("2 - Refined Sort Merge Join");
			System.out.println("3 - Hash Join");
			System.out.println("4 - Exit");
			System.out.println("");
			System.out.println("");
			System.out.print("Select a Menu Option: ");
			try {
				// get input
				int opt = Integer.parseInt(in.next());

				Algorithms.testCases(opt);

			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		} while (true);
	}
}
