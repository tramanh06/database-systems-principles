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

		// Condition check
		if (rel.getNumBlocks() / Setting.memorySize >= (Setting.memorySize - 1)) {
			System.out.println("Error! Assumption 2 is not satisfied.");
			return 0;
		}

		// Phase 1: Generating sublists to disk
		RelationLoader rLoader = rel.getRelationLoader();
		int memorySize = Setting.memorySize;
		ArrayList<Relation> sublists = new ArrayList<>();

		while (rLoader.hasNextBlock()) {
			System.out.println("--->Load at most M blocks into memory...");
			Block[] blocks = rLoader.loadNextBlocks(memorySize);

			// Statistics: cost to read from disk
			// Assume: every read will read M blocks from disk
			// TODO: confirm with prof with assumption
			numIO += memorySize;

			ArrayList<Tuple> tempTuples = new ArrayList<>();

			// Add all tuples to memory
			for (Block b : blocks) {
				if (b != null) {
					tempTuples.addAll(b.tupleLst);
					b.print(false);
				}
			}
			// Sort tuples in memory
			sortTuples(tempTuples);

			// Write to sublist
			Relation sublist = writeTuplesToRelation("sublist", tempTuples);
			sublists.add(sublist);

			// Statistics: cost to write sublist to disk
			numIO += sublist.getNumBlocks();
		}
		System.out.println("Number of sublists written to disk: " + sublists.size());

		////// Phase 2: Merge sort
		
		Relation result = new Relation(relName);
//		ArrayList<Block> blockBuffers = new ArrayList<>();
		Block[] blockBuffers = new Block[Setting.memorySize-1];
		Block blockOutput = new Block();
		boolean hasTuple = false;
		
		// Load first block of each sublist to blockBuffers
		int i=0;
		ArrayList<RelationLoader> subLoaders = new ArrayList<>();
		RelationLoader tempLoader;
		for (Relation sublist : sublists) {
			tempLoader = sublist.getRelationLoader();
			if (tempLoader.hasNextBlock()) {
//				blockBuffers.add(sublist.getRelationLoader().loadNextBlocks(1)[0]);
				blockBuffers[i] = tempLoader.loadNextBlocks(1)[0];
				hasTuple = true;
			}
			subLoaders.add(tempLoader);
			i++;
		}
		while (hasTuple) {
			Tuple smallestTuple = getSmallestTuple(blockBuffers, subLoaders);
			
			//print tuple
//			System.out.println(smallestTuple.toString());
			// Print to console + modify Relation rel
			if (!blockOutput.insertTuple(smallestTuple)) {
//				System.out.println("Finish a block");
				result.getRelationWriter().writeBlock(blockOutput);		// write to Relation result
				numIO++; // IO Cost to write to disk
				blockOutput = new Block();
				blockOutput.insertTuple(smallestTuple);
			}
			
			hasTuple = false;
			for (Relation sublist: sublists){
				if(sublist.getNumTuples()!=0)
					hasTuple = true;
			}
		}
		result.getRelationWriter().writeBlock(blockOutput);		// write last block

		System.out.println("Result Relation: ");
		result.printRelation(true, true);
		
//		rel = result;	Doesn't work
		

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

	private Relation writeTuplesToRelation(String relName, ArrayList<Tuple> tuples) {
		Relation rel = new Relation(relName);
		Block tempBlock = new Block();
		for (Tuple t : tuples) {
			if (!tempBlock.insertTuple(t)) {
				rel.getRelationWriter().writeBlock(tempBlock);
				// numIO++; // IO Cost to write
				tempBlock = new Block();
				tempBlock.insertTuple(t);
			}
		}
		rel.getRelationWriter().writeBlock(tempBlock);

		return rel;
	}

	private Tuple getSmallestTuple(Block[] blockBuffers, ArrayList<RelationLoader> subLoaders) {
		Tuple temp = null;
		int minIndex = 0;
		int i = 0;
		
		for (Block block: blockBuffers){
			if(block!=null && block.getNumTuples() != 0){
				temp = block.tupleLst.get(0);
				break;
			}
//			return null;
		}
		
		for (Block block : blockBuffers) {
			if(block!=null && block.getNumTuples() != 0){
				Tuple firstTuple = block.tupleLst.get(0);
				if(firstTuple.key < temp.key) {
					temp = firstTuple;
					minIndex = i;
				}
			}
			i++;
		}

		Tuple smallestTuple = blockBuffers[minIndex].tupleLst.remove(0);
		// check if block is empty. If yes, load next block
		if (blockBuffers[minIndex].getNumTuples() == 0) {
//			System.out.println("Block is empty");
			if(subLoaders.get(minIndex).hasNextBlock()){
				Block nextblock = subLoaders.get(minIndex).loadNextBlocks(1)[0];
				blockBuffers[minIndex] = nextblock;
			}
		}

		return smallestTuple;
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

	public int refinedSortMergeJoinRelations(Relation relR, Relation relS, Relation relRS) {
		int numIO = 0;

		// Insert your code here!

		return numIO;
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

		/* MergeSortRelation */
		int MSCost = algo.mergeSortRelation(relR);
		System.out.println("FOr debug");
		// Insert your test cases here!

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
