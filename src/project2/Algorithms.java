package project2;
/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2. This set of code shall NOT be redistributed.
 * You should provide implementation for the three algorithms declared in this class.  
 */

//package project2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import project2.Relation.RelationLoader;
import project2.Relation.RelationWriter;

public class Algorithms {
	// Note about the relation R and S:
	// There are duplicates for one key. With different name.

	/**
	 * Sort the relation using Setting.memorySize buffers of memory
	 * 
	 * @param rel
	 *            is the relation to be sorted.
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int mergeSortRelation(Relation rel) {
		int numIO = 0;
		String relName = rel.name;

		/* Condition check */
		if ( rel.getNumBlocks() / Setting.memorySize >= (Setting.memorySize - 1)) {
			System.out.println("Error! Assumption 2 is not satisfied.");
			return -1;
		}

		/* 
		 * Phase 1: Generating sublists to disk 
		 */
		RelationLoader rLoader = rel.getRelationLoader();
		ArrayList<Relation> sublists = new ArrayList<>();
		numIO += createSublists(rLoader, sublists);
		
		System.out.println("Number of sublists written to disk: " + sublists.size());

		/* 
		 * Phase 2: Merge sort
		 */
		Relation result = new Relation(relName);
		Block[] blockBuffers = new Block[Setting.memorySize-1];	/* Simulate block buffers in memory */
		Block blockOutput = new Block();	/* Simulate single block output in memory */
		boolean hasTuple = true;	/* Condition to stop while loop below, assume all relations are valid */
		
		/* Load first block of each sublist to blockBuffers */
		ArrayList<RelationLoader> subLoaders = new ArrayList<>();
		numIO += loadFirstBlocksToMem(sublists, blockBuffers, subLoaders);
		
		/* Merge sublists */
		while (hasTuple) {
			Tuple smallestTuple = getSmallestTuple(blockBuffers, subLoaders);
			/* Insert smallestTuple to blockOutput */
			if (!blockOutput.insertTuple(smallestTuple)) {
				result.getRelationWriter().writeBlock(blockOutput);		// write to Relation result
				numIO++; // IO Cost to write to disk
				blockOutput = new Block();
				blockOutput.insertTuple(smallestTuple);
			}
			
			/* Check if there is any tuple left */
			hasTuple = false;
			for (Relation sublist: sublists){
				if(sublist.getNumTuples()!=0){
					hasTuple = true;
					break;
				}
			}
		}
		result.getRelationWriter().writeBlock(blockOutput);		/* write last block */
		numIO++;
		
		/* Copy tuplelst from result to original rel */
		copyToRel(result, rel);
		
		return numIO;
	}
	
	/**
	 * Create sublists according to block size of memory and block factor
	 * @param rLoader
	 * 				RelationLoader of the relation that's to be used to create sublists
	 * @param sublists
	 * 				Resulted sublists 
	 * @return
	 * 				
	 */
	private int createSublists(RelationLoader rLoader, ArrayList<Relation> sublists){
		int numIO = 0;
		while (rLoader.hasNextBlock()) {
			Block[] blocks = rLoader.loadNextBlocks(Setting.memorySize);

			/* Statistics: cost to read from disk
			 * Assume: every read will read M blocks from disk, even the last block.
			 * Update: wrong! must count number of non-null elements in array
			 * TODO: fix this
			 */
			numIO += blocks.length;

			ArrayList<Tuple> tempTuples = new ArrayList<>();

			/* Add all tuples to memory */
			for (Block b : blocks) {
				if (b != null) {
					tempTuples.addAll(b.tupleLst);
					b.print(false);
				}
			}
			/* Sort tuples in memory */
			sortTuples(tempTuples);

			/* Write to sublist */
			Relation sublist = new Relation("sublist"); 
			numIO += writeTuplesToRelation(sublist, tempTuples);
			sublists.add(sublist);
		}
		return numIO;
	}

	private int loadFirstBlocksToMem(ArrayList<Relation> sublists, Block[] blockBuffers, ArrayList<RelationLoader> subLoaders){
		int i=0;
		int numIO = 0;
		RelationLoader tempLoader;
		
		for (Relation sublist : sublists) {
			tempLoader = sublist.getRelationLoader();
			if (tempLoader.hasNextBlock()) {
				blockBuffers[i] = tempLoader.loadNextBlocks(1)[0];
			}
			subLoaders.add(tempLoader);
			numIO += sublist.getNumBlocks();	/* Cost of eventual loading blocks of each sublist */
			i++;
		}
		return numIO;
	}
	
	private ArrayList<Tuple> sortTuples(ArrayList<Tuple> tuples) {
		Collections.sort(tuples, (Tuple one, Tuple other) -> one.key - other.key);
		// To test
		// for(Tuple t:tuples)
		// {
		// System.out.println(t.toString());
		// }
		return tuples;
	}

	private int writeTuplesToRelation(Relation rel, ArrayList<Tuple> tuples) {
		int numIO=0;
		Block tempBlock = new Block();
		for (Tuple t : tuples) {
			if (!tempBlock.insertTuple(t)) {
				rel.getRelationWriter().writeBlock(tempBlock);
				numIO++; // IO Cost to write
				tempBlock = new Block();
				tempBlock.insertTuple(t);
			}
		}
		rel.getRelationWriter().writeBlock(tempBlock);
		numIO++;
		return numIO;
	}

	private Tuple getSmallestTuple(Block[] blockBuffers, ArrayList<RelationLoader> subLoaders) {

		int minIndex = getMinBlockIndex(blockBuffers);
		Tuple smallestTuple = blockBuffers[minIndex].tupleLst.remove(0);
		
		// check if block is empty. If yes, load next block
		if (blockBuffers[minIndex].getNumTuples() == 0) {
			if(subLoaders.get(minIndex).hasNextBlock()){
				Block nextblock = subLoaders.get(minIndex).loadNextBlocks(1)[0];
				blockBuffers[minIndex] = nextblock;
			}
		}

		return smallestTuple;
	}
	
	private int getMinBlockIndex(Block[] blockBuffers){
		Tuple temp = null;
		int minIndex = -1;
		int i = 0;
		int flag = -1;
		for (Block block : blockBuffers) {
			if(block!=null && block.getNumTuples() != 0){
				if(flag == -1){
					temp = block.tupleLst.get(0);
					minIndex = i;
					flag = 0;
				}
					
				Tuple firstTuple = block.tupleLst.get(0);
				if(firstTuple.key < temp.key) {
					temp = firstTuple;
					minIndex = i;
				}
			}
			i++;
		}
		return minIndex;
	}

	private void copyToRel(Relation result, Relation rel){
		RelationLoader resLoader = result.getRelationLoader();
		RelationLoader relLoader = rel.getRelationLoader();
		
		while(resLoader.hasNextBlock()){
			relLoader.loadNextBlocks(1)[0].tupleLst = resLoader.loadNextBlocks(1)[0].tupleLst;
		}
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

		/* Phase 1: Hash to M-1 buckets */
		RelationLoader RLoader = relR.getRelationLoader();
		RelationLoader SLoader = relS.getRelationLoader();
		
		Relation[] RSublists = new Relation[Setting.memorySize-1];
		Relation[] SSublists = new Relation[Setting.memorySize-1];
		/* Initialize sublists*/
		initializeSublists(RSublists, "RSublist");
		initializeSublists(SSublists, "SSublist");
		/* Hash each relation */
		numIO += hashByBlock(RLoader, RSublists);
		numIO += hashByBlock(SLoader, SSublists);
		
		/* Phase 2: Compare each sublist from R to each from S*/
		ArrayList<Tuple> results = new ArrayList<>();
		for(int i=0; i<Setting.memorySize-1; i++){
			join2sublists(RSublists[i].getRelationLoader(), SSublists[i].getRelationLoader(), results);
		}
		
		writeTuplesToRelation(relRS, results);

		return numIO;
	}
	
	private void initializeSublists(Relation[] sublists, String sublistName){
		for(int i=0; i<Setting.memorySize-1; i++){
			sublists[i] = new Relation(sublistName);
		}
	}
	
	private int hashByBlock(RelationLoader rLoader, Relation[] hashedSublists){
		int numIO = 0;
		int hashValue;
		
		Block[] hashMem = new Block[Setting.memorySize-1];
		for(int i=0; i<Setting.memorySize-1; i++){
			hashMem[i] = new Block();
		}
		
		while(rLoader.hasNextBlock()){
			Block inBuffer = rLoader.loadNextBlocks(1)[0];
			for(Tuple each: inBuffer.tupleLst){
				hashValue = each.key%(Setting.memorySize-1);
				if(!hashMem[hashValue].insertTuple(each)){
					hashedSublists[hashValue].getRelationWriter().writeBlock(hashMem[hashValue]);
					numIO++; // IO Cost to write
					hashMem[hashValue] = new Block();
					hashMem[hashValue].insertTuple(each);
				}
			}
		}
		
		/* Write to disk last block from memory */
		for(int i=0; i<Setting.memorySize-1; i++){
			if(hashMem[i] != null){
				hashedSublists[i].getRelationWriter().writeBlock(hashMem[i]);
				numIO++;
			}
		}
		
		// Test
//		for(Relation each: hashedSublists){
//			each.printRelation(true, true);
//		}
		
		return numIO;
	}

	private void join2sublists(RelationLoader RLoader, RelationLoader SLoader, ArrayList<Tuple> results){
		/* Load R into M-1 buffers. Assume R's size is smaller */
		Block[] leftBuffers = new Block[Setting.memorySize-1];
		Block rightBuffer;
		while(RLoader.hasNextBlock()){
			leftBuffers = RLoader.loadNextBlocks(Setting.memorySize-1);
			SLoader.reset();
			while(SLoader.hasNextBlock()){
				rightBuffer = SLoader.loadNextBlocks(1)[0];
				
				for(Block lBlock: leftBuffers){
					if(lBlock != null){
						for(Tuple lTuple: lBlock.tupleLst){
							for(Tuple rTuple: rightBuffer.tupleLst){
								if(lTuple.key == rTuple.key){
									Tuple temp = new Tuple(lTuple.key, String.format("(%s, %s)", lTuple.value, rTuple.value));
									results.add(temp);
								}
							}
						}
					}
				}
			}
		}
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

	public int refinedSortMergeJoinRelations(Relation relR, Relation relS, Relation relRS) {
		int numIO = 0;

		/* Condition checking */
		if (relR.getNumBlocks()/Setting.memorySize + relS.getNumBlocks()/Setting.memorySize > Setting.memorySize-1){
			System.out.println("Condition for Refined SortMerge Join is not met. Program is exiting..");
			return -1;
		}
			
		/* Phase 1: Creating sorted sublists for R and S */
		ArrayList<Relation> Rsublists = new ArrayList<>();
		numIO += createSublists(relR.getRelationLoader(), Rsublists);
		
		ArrayList<Relation> Ssublists = new ArrayList<>();
		numIO += createSublists(relS.getRelationLoader(), Ssublists);
		
		/* Phase 2 */
		/* Load first block of each sublist to blockBuffers */
		Block[] RblockBuffers = new Block[Rsublists.size()];	/* Simulate block buffers in memory */
		Block[] SblockBuffers = new Block[Ssublists.size()];
		Block blockOutput = new Block();	/* Simulate single block output in memory */
		boolean hasTuple = false;	/* Condition to stop while loop below */
		
		ArrayList<RelationLoader> RsubLoaders = new ArrayList<>();
		ArrayList<RelationLoader> SsubLoaders = new ArrayList<>();
		numIO += loadFirstBlocksToMem(Rsublists, RblockBuffers, RsubLoaders);
		numIO += loadFirstBlocksToMem(Ssublists, SblockBuffers, SsubLoaders);
		
		ArrayList<Tuple> results = new ArrayList<>();
		while(true){
			/* Get smallest tuple from each sublist*/
			int minIndexR = getMinBlockIndex(RblockBuffers);
			int minIndexS = getMinBlockIndex(SblockBuffers);
			if(minIndexR == -1 || minIndexS == -1)
				break;
			Tuple Rsmallest = RblockBuffers[minIndexR].tupleLst.get(0);
			Tuple Ssmallest = SblockBuffers[minIndexS].tupleLst.get(0);
			
			if(Rsmallest.key == Ssmallest.key){
				numIO += joinSmallestTuples(Rsmallest.key, RblockBuffers, SblockBuffers, RsubLoaders, SsubLoaders, results);
				
			}
			else if (Rsmallest.key < Ssmallest.key){
				/* Remove Rsmallest */
				RblockBuffers[minIndexR].tupleLst.remove(0);
				if(RblockBuffers[minIndexR].getNumTuples()==0 && RsubLoaders.get(minIndexR).hasNextBlock()){
					RblockBuffers[minIndexR] = RsubLoaders.get(minIndexR).loadNextBlocks(1)[0];
					numIO++;
				}
				
			}
			else if (Ssmallest.key < Rsmallest.key){
				/* Remove Ssmallest */
				SblockBuffers[minIndexS].tupleLst.remove(0);
				if(SblockBuffers[minIndexS].getNumTuples()==0 && SsubLoaders.get(minIndexS).hasNextBlock()){
					SblockBuffers[minIndexS] = SsubLoaders.get(minIndexS).loadNextBlocks(1)[0];
					numIO++;
				}
			}
		}
		
		writeTuplesToRelation(relRS, results);
		return numIO;
	}
	
	private int joinSmallestTuples(int smallestKey, Block[] RblockBuffers, Block[] SblockBuffers,
			ArrayList<RelationLoader> RsubLoaders, ArrayList<RelationLoader> SsubLoaders, ArrayList<Tuple> results){
		int numIO=0;
		
		/* RSmallestTuples: List of Tuples that contain smallestKey */
		ArrayList<Tuple> RSmallestTuples = new ArrayList<>();
		ArrayList<Tuple> SSmallestTuples = new ArrayList<>();
		
		numIO += extractTuplesWithKey(smallestKey, RblockBuffers, RsubLoaders, RSmallestTuples);
		numIO += extractTuplesWithKey(smallestKey, SblockBuffers, SsubLoaders, SSmallestTuples);
		
		cartesianJoin(RSmallestTuples, SSmallestTuples, results);
		
		return numIO;
	}
	
	private int extractTuplesWithKey(int smallestKey, Block[] blockBuffers, ArrayList<RelationLoader> subLoaders,
									ArrayList<Tuple> smallestTuples){
		int numIO=0;
		
		int i=0;
		for(Block b: blockBuffers){
			if(b != null){
				for (Iterator<Tuple> tuples = b.tupleLst.iterator(); tuples.hasNext();) {
				    Tuple t = tuples.next();
					if(t.key != smallestKey)
						break;
					else{
						if(t.key == smallestKey){
							smallestTuples.add(t);
							tuples.remove();	// Remove current iterator
							
							if(b.getNumTuples()==0 && subLoaders.get(i).hasNextBlock()){
								blockBuffers[i] = subLoaders.get(i).loadNextBlocks(1)[0];
								numIO++;
							}
						}
					}
				}
			}
			
			i++;
		}
		return numIO;
	}
	
	private void cartesianJoin(ArrayList<Tuple> RSmallestTuples, ArrayList<Tuple> SSmallestTuples, ArrayList<Tuple> results){
		for(Tuple rTuple: RSmallestTuples)
			for(Tuple sTuple: SSmallestTuples){
				Tuple temp = new Tuple(rTuple.key, String.format("(%s, %s)", rTuple.value, sTuple.value));
				results.add(temp);
			}
	}
	/**
	 * Example usage of classes.
	 */
	public static void examples() {

		/* Populate relations */
		System.out.println("---------Populating two relations----------");
		Relation relR = new Relation("RelR");
		int numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains " + numTuples + " tuples.");
		Relation relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains " + numTuples + " tuples.");
		System.out.println("---------Finish populating relations----------\n\n");

		/* Print the relation */
		System.out.println("---------Printing relations----------");
		relR.printRelation(true, true);
		relS.printRelation(true, false);
		System.out.println("---------Finish printing relations----------\n\n");

		/* Example use of RelationLoader */
		System.out.println("---------Loading relation RelR using RelationLoader----------");
		RelationLoader rLoader = relR.getRelationLoader();
		while (rLoader.hasNextBlock()) {
			System.out.println("--->Load at most 7 blocks each time into memory...");
			Block[] blocks = rLoader.loadNextBlocks(7);
			// print out loaded blocks
			for (Block b : blocks) {
				if (b != null)
					b.print(false);
			}
		}
		System.out.println("---------Finish loading relation RelR----------\n\n");

		/* Example use of RelationWriter */
		System.out.println("---------Writing to relation RelS----------");
		RelationWriter sWriter = relS.getRelationWriter();
		rLoader.reset();
		if (rLoader.hasNextBlock()) {
			System.out.println("Writing the first 7 blocks from RelR to RelS");
			System.out.println("--------Before writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);

			Block[] blocks = rLoader.loadNextBlocks(7);
			for (Block b : blocks) {
				if (b != null)
					sWriter.writeBlock(b);
			}
			System.out.println("--------After writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);
		}

	}

	/**
	 * Testing cases.
	 */
	public static void testCases() {
		Algorithms algo = new Algorithms();

		/* Populate relation */
		System.out.println("---------Populating two relations----------");
		Relation relR = new Relation("RelR");
		int numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains " + numTuples + " tuples.");
		Relation relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains " + numTuples + " tuples.");
		System.out.println("---------Finish populating relations----------\n\n");

		/* MergeSortRelation */
//		System.out.println("-----Test Merge Sort Algorithm------");
//		int MSCost = algo.mergeSortRelation(relS);
//		relS.printRelation(true, true);
//		System.out.println("NumIO = "+MSCost);
		
		/* Refined Sort-Merge */
		Relation relRS = new Relation("RelRS");
		int RSMCost = algo.refinedSortMergeJoinRelations(relR, relS, relRS);
		relRS.printRelation(true, true);
		System.out.println("Num Tuples="+relRS.getNumTuples());
		
		/* HashJoinRelation */
//		System.out.println("-----Test HashJoin Algorithm------");
//		Relation relRS = new Relation("RelRS");
//		System.out.println("Num tuples in R: "+relR.getNumTuples());
//		int IOCost = algo.hashJoinRelations(relR, relS, relRS);
//		System.out.println("NumIO: "+IOCost);
//		relRS.printRelation(true, true);
//		System.out.println("Num Tuples="+relRS.getNumTuples());
//		
//		// Sort relRS to match with result
//		System.out.println("------Relation after sort-------");
//		algo.mergeSortRelation(relRS);
//		relRS.printRelation(true, true);
//		System.out.println("Num Tuples="+relRS.getNumTuples());
	}

	/**
	 * This main method provided for testing purpose
	 * 
	 * @param arg
	 */
	public static void main(String[] arg) {
		Algorithms.testCases();
		// examples();
	}
}
