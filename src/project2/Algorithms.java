package project2;
/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2. This set of code shall NOT be redistributed.
 * You should provide implementation for the three algorithms declared in this class.  
 */

//package project2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
			System.out.println("MergeSort Error! Assumption 2 is not satisfied.");
			return -1;
		}

		/* 
		 * Phase 1: Generating sublists to disk 
		 */
		RelationLoader rLoader = rel.getRelationLoader();
		ArrayList<Relation> sublists = new ArrayList<>();
		numIO += createSublists(rLoader, sublists);
		
		sublistsStats(sublists);

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
			Tuple smallestTuple = new Tuple(-1,"");	// Dummy initialization, will override in getSmallestTuple()
			numIO += getSmallestTuple(blockBuffers, subLoaders, smallestTuple);
			
			/* Insert smallestTuple to blockOutput */
			if (!blockOutput.insertTuple(smallestTuple)) {
				result.getRelationWriter().writeBlock(blockOutput);		// write to Relation result
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
			ArrayList<Tuple> tempTuples = new ArrayList<>();

			/* Add all tuples to memory */
			for (Block b : blocks) {
				if (b != null) {
					tempTuples.addAll(b.tupleLst);
					numIO++;
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

	private int loadFirstBlocksToMem(ArrayList<Relation> sublists, 
			Block[] blockBuffers, ArrayList<RelationLoader> subLoaders){
		int i=0;
		int numIO = 0;
		RelationLoader tempLoader;
		
		for (Relation sublist : sublists) {
			tempLoader = sublist.getRelationLoader();
			if (tempLoader.hasNextBlock()) {
				blockBuffers[i] = tempLoader.loadNextBlocks(1)[0];
			}
			subLoaders.add(tempLoader);
			numIO ++;	/* Cost of loading block */
			i++;
		}
		return numIO;
	}
	
	private ArrayList<Tuple> sortTuples(ArrayList<Tuple> tuples) {
		Collections.sort(tuples, (Tuple one, Tuple other) -> one.key - other.key);
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

	private int getSmallestTuple(Block[] blockBuffers, ArrayList<RelationLoader> subLoaders, Tuple smallestTuple) {
		int numIO=0;
		int minIndex = getMinBlockIndex(blockBuffers);
		Tuple temp = blockBuffers[minIndex].tupleLst.remove(0);
		smallestTuple.key = temp.key;
		smallestTuple.value = temp.value;
		
		// check if block is empty. If yes, load next block
		if (blockBuffers[minIndex].getNumTuples() == 0) {
			if(subLoaders.get(minIndex).hasNextBlock()){
				Block nextblock = subLoaders.get(minIndex).loadNextBlocks(1)[0];
				blockBuffers[minIndex] = nextblock;
				numIO++;
			}
		}
		return numIO;
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
		
		return minIndex;	// Return -1 if blockBuffers is empty
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
		
		if(Math.min(relR.getNumBlocks()/Setting.memorySize, relS.getNumBlocks()/Setting.memorySize)>Setting.memorySize-1){
			System.out.println("HashJoin Error! Average bucket size is more than M-1. Program is exiting...");
			return -1;
		}
			
		/* Phase 1: Hash to M-1 buckets */
		RelationLoader RLoader = relR.getRelationLoader();
		RelationLoader SLoader = relS.getRelationLoader();
		
		Relation[] RSublists = new Relation[Setting.memorySize-1];
		Relation[] SSublists = new Relation[Setting.memorySize-1];
		/* Initialize sublists*/
		initializeSublists(RSublists, "RSublist");
		initializeSublists(SSublists, "SSublist");
		/* Hash each relation & write to disk*/
		numIO += hashByBlock(RLoader, RSublists);
		numIO += hashByBlock(SLoader, SSublists);
		
//		System.out.println("NumIO="+numIO);		// Note: this IO may not be equal to 2(B(R)+B(S)), 
												// because hashed sublists are not condensed
		ArrayList<Relation> RSublists_AL = new ArrayList<Relation>(Arrays.asList(RSublists));
		ArrayList<Relation> SSublists_AL = new ArrayList<Relation>(Arrays.asList(SSublists));
		System.out.println("Relation R buckets: ");
		sublistsStats(RSublists_AL);
		System.out.println("Relation S buckets: ");
		sublistsStats(SSublists_AL);
		
		
		/* Phase 2: Compare each sublist from R to each from S*/
		ArrayList<Tuple> results = new ArrayList<>();
		for(int i=0; i<Setting.memorySize-1; i++){
			numIO += join2sublists(RSublists[i].getRelationLoader(), SSublists[i].getRelationLoader(), results);
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
			numIO++;	// Cost to read
			for(Tuple each: inBuffer.tupleLst){
				hashValue = each.key%(Setting.memorySize-1);
				if(!hashMem[hashValue].insertTuple(each)){
					hashedSublists[hashValue].getRelationWriter().writeBlock(hashMem[hashValue]);
					numIO++; // Cost to write
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
		
		return numIO;
	}

	private int join2sublists(RelationLoader RLoader, RelationLoader SLoader, ArrayList<Tuple> results){
		int leftIO=0, rightIO=0;
		/* Load R into M-1 buffers. Assume R's size is smaller */
		Block[] leftBuffers = new Block[Setting.memorySize-1];
		Block rightBuffer;
		if(RLoader.hasNextBlock()){		// Assume Rsublist can fit into memory (M-1 buffers)
			leftBuffers = RLoader.loadNextBlocks(Setting.memorySize-1);
			SLoader.reset();
			while(SLoader.hasNextBlock()){
				rightBuffer = SLoader.loadNextBlocks(1)[0];
				rightIO++;	// Cost to load rightBuffer
				
				for(Block lBlock: leftBuffers){
					if(lBlock != null){
						leftIO++;	// Cost to read leftBuffers, repeated for each rightBuffer
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
		return rightIO+leftIO/rightIO;
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
		
		// Sublists statistics
		System.out.println("Sublists statistics for RelR:");
		sublistsStats(Rsublists);
		System.out.println("Sublists statistics for RelS:");
		sublistsStats(Ssublists);
		
		/* Phase 2 */
		/* Load first block of each sublist to blockBuffers */
		Block[] RblockBuffers = new Block[Rsublists.size()];	/* Simulate block buffers in memory */
		Block[] SblockBuffers = new Block[Ssublists.size()];
		
		ArrayList<RelationLoader> RsubLoaders = new ArrayList<>();
		ArrayList<RelationLoader> SsubLoaders = new ArrayList<>();
		numIO += loadFirstBlocksToMem(Rsublists, RblockBuffers, RsubLoaders);
		numIO += loadFirstBlocksToMem(Ssublists, SblockBuffers, SsubLoaders);
		
		ArrayList<Tuple> results = new ArrayList<>();
		while(true){
			/* Get smallest tuple from each sublist*/
			int minIndexR = getMinBlockIndex(RblockBuffers);
			int minIndexS = getMinBlockIndex(SblockBuffers);
			if(minIndexR == -1 || minIndexS == -1){
				break;
			}
			Tuple Rsmallest = RblockBuffers[minIndexR].tupleLst.get(0);
			Tuple Ssmallest = SblockBuffers[minIndexS].tupleLst.get(0);
			
			if(Rsmallest.key == Ssmallest.key){
				numIO += joinSmallestTuples(Rsmallest.key, RblockBuffers, SblockBuffers, RsubLoaders, SsubLoaders, results);
				
			}
			else if (Rsmallest.key < Ssmallest.key){
				/* Remove Rsmallest */
				RblockBuffers[minIndexR].tupleLst.remove(0);
				// load next block if block is empty
				if(RblockBuffers[minIndexR].getNumTuples()==0 && RsubLoaders.get(minIndexR).hasNextBlock()){
					RblockBuffers[minIndexR] = RsubLoaders.get(minIndexR).loadNextBlocks(1)[0];
					numIO++;
				}
				
			}
			else if (Ssmallest.key < Rsmallest.key){
				/* Remove Ssmallest */
				SblockBuffers[minIndexS].tupleLst.remove(0);
				// load next block if block is empty
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
		
		/* RSmallestTuples: List of all Tuples from R that contain smallestKey */
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
			while(b!=null && b.getNumTuples()!=0){
				Iterator<Tuple> tuples = b.tupleLst.iterator();
			    while (tuples.hasNext()) {
			    	Tuple t = tuples.next();
			    	if(t.key != smallestKey){
			    		b = null; 	// to stop while(b!=null)
			    		break;
			    	}
			    	else {
						smallestTuples.add(t);
						tuples.remove();	// Remove current iterator
						
						if(b.getNumTuples()==0 && subLoaders.get(i).hasNextBlock()){
							blockBuffers[i] = subLoaders.get(i).loadNextBlocks(1)[0];
							b = blockBuffers[i];
							numIO++;
						}
					}
				}
			}
			i++;
		}
		return numIO;
	}
	
	private void cartesianJoin(ArrayList<Tuple> RSmallestTuples, ArrayList<Tuple> SSmallestTuples, 
			ArrayList<Tuple> results){
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

	private void sublistsStats(ArrayList<Relation> sublists){
		ArrayList<Integer> stat_blocks = new ArrayList<>();
		ArrayList<Integer> stat_tuples = new ArrayList<>();
		float sum_blocks=0;
		float sum_tuples=0;
		
		for(Relation sublist : sublists){
			stat_blocks.add(sublist.getNumBlocks());
			sum_blocks += sublist.getNumBlocks();
			stat_tuples.add(sublist.getNumTuples());
			sum_tuples += sublist.getNumTuples();
		}
		
		System.out.println("Sublists statistics: ");
		System.out.println("Total Sublists:\t"+sublists.size());
		System.out.println("\t\tMax length|Min Length|Avg Length");
		System.out.println(String.format("In blocks:\t\t%s\t%s\t%s", 
				Collections.max(stat_blocks), Collections.min(stat_blocks), sum_blocks/sublists.size()));
		System.out.println(String.format("In Tuples:\t\t%s\t%s\t%s", 
				Collections.max(stat_tuples), Collections.min(stat_tuples), sum_tuples/sublists.size()));
	}
	
	/**
	 * Testing cases Merge.
	 */	
	private void testCasesMerge() {
		Algorithms algorithm = new Algorithms();

		// Test Case 
		for(int i=20; i>=0; i--){
			Setting.blockFactor = 20;
			Setting.memorySize = i;
			algorithm.testMerge(21-i, "Minimum Memory");
		}
	}
	private void testMerge(int cases, String testType) {
		int numIO;
		int numTuples;
		Relation relR;
		String relName = "RelR";

		relR = new Relation(relName);
		numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation "+relName+" contains " + numTuples + " tuples.");
		System.out.println("------TestCase " + cases + ": " + testType);
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		int IOCostTheory = 3*(relR.getNumBlocks());
		
		numIO = mergeSortRelation(relR);
		if (numIO != -1) {
			System.out.println("\nMerge Sort: IO Theory: "+IOCostTheory+"	Actual IO Cost : " + numIO);
			relR.printRelation(false, false);
		}
		System.out.println("\n");
	}

	/**
	 * Test cases for Refined Sort Merge join
	 */
	private void testcasesRSMJ(){
		Algorithms algorithm = new Algorithms();

		// Test Case 
		for(int i=20; i>=0; i--){
			Setting.blockFactor = 20;
			Setting.memorySize = i;
			algorithm.testRefinedSMJ(21-i, "Minimum Memory Size");
		}
	}
	private void testRefinedSMJ(int cases, String testType){
		/* Populate relation */
		Relation relR = new Relation("RelR");
		relR.populateRelationFromFile("RelR.txt");
		relR.printRelation(false, false);
		
		Relation relS = new Relation("RelS");
		relS.populateRelationFromFile("RelS.txt");
		relS.printRelation(false, false);
		
		System.out.println("------TestCase " + cases + ": " + testType);
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		
		Relation relRS = new Relation("RelRS");
		int numIO = refinedSortMergeJoinRelations(relR, relS, relRS);
		int IOCostTheory = 3*(relR.getNumBlocks()+relS.getNumBlocks());
		if (numIO != -1) {
			System.out.println("\nRefined Sort Merge Join: IO Theory: "+IOCostTheory+"	Actual IO Cost : " + numIO);
			relRS.printRelation(false, false);
		}
		System.out.println("\n");
	}
	
	private void testcasesHJ(){
		Algorithms algorithm = new Algorithms();

		// Test Case 
		for(int i=20; i>=0; i--){
			Setting.blockFactor = i;
			Setting.memorySize = 20;
			algorithm.testHJ(21-i, "Minimum Block Factor");
		}
	}
	
	private void testHJ(int cases, String testType){
		/* Populate relation */
		Relation relR = new Relation("RelR");
		relR.populateRelationFromFile("RelR.txt");
		relR.printRelation(false, false);
		
		Relation relS = new Relation("RelS");
		relS.populateRelationFromFile("RelS.txt");
		relS.printRelation(false, false);
		
		System.out.println("------TestCase " + cases + ": " + testType);
		System.out.println("Memory Size = " + Setting.memorySize + ", Block Factor = " + Setting.blockFactor);
		
		Relation relRS = new Relation("RelRS");
		int numIO = hashJoinRelations(relR, relS, relRS);
		int IOCostTheory = 3*(relR.getNumBlocks()+relS.getNumBlocks());
		if (numIO != -1) {
			System.out.println("\nHash Join: IO Theory: "+IOCostTheory+"	Actual IO Cost : " + numIO);
			relRS.printRelation(false, false);
		}
		System.out.println("\n");
	}
	
	/**
	 * Testing cases.
	 */
	private static void testCases() {
		Algorithms algo = new Algorithms();

		/* MergeSortRelation */
		algo.testCasesMerge();
		
		/* Refined Sort-Merge */
//		algo.testcasesRSMJ();

		/* HashJoinRelation */
//		algo.testcasesHJ();
	}

	/**
	 * This main method provided for testing purpose
	 * 
	 * @param arg
	 */
	public static void main(String[] arg) {
		Algorithms.testCases();
	}
}
