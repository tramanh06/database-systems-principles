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
	 * @param rel is the relation to be sorted. 
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public static int mergeSortRelation(Relation rel){
		int numIO=0;
		
		//Insert your code here!
                // Merge Sort
                RelationLoader rLoader=rel.getRelationLoader();	
                int memorySize = Setting.memorySize;
		while(rLoader.hasNextBlock()){
                       
			System.out.println("--->Load at most M blocks into memory...");
			Block[] blocks=rLoader.loadNextBlocks(memorySize);
                        ArrayList<Tuple> tempTuples = new ArrayList<>();
			// Add all tuples to memory
			for(Block b:blocks){
				if(b!=null) {
                                    tempTuples.addAll(b.tupleLst);
                                    b.print(false);
                                }
			}
                        // Sort tuples in memory 
                        sortTuples(tempTuples);
                        
                        // Write to sublist
                        Relation sublist = new Relation("sublist");
                        Block tempBlock = new Block();
                        for(Tuple t: tempTuples){
                            if (!tempBlock.insertTuple(t)) {
					sublist.getRelationWriter().writeBlock(tempBlock);
					tempBlock = new Block();
					tempBlock.insertTuple(t);
                            }                            
                        }
                        sublist.getRelationWriter().writeBlock(tempBlock);
                        System.out.println("FOr debug");
                        
		}
//                System.out.println("--->Load 1 block into memory...");
//                Block[] block=rLoader.loadNextBlocks(1);
//                block[0].print(true);
//                ArrayList<Tuple> tuples = block[0].tupleLst;
//                System.out.println("After sorting: ");

                
		
		return numIO;
	}
        
        private static ArrayList<Tuple> sortTuples(ArrayList<Tuple> tuples)
        {
            Collections.sort(tuples, (Tuple one, Tuple other) -> one.key - other.key);
            // To test
            for(Tuple t:tuples)
            {
                System.out.println(t.toString());
            }
            return tuples;
        }
       
	
	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory to produce the result relation relRS
	 * @param relR is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int hashJoinRelations(Relation relR, Relation relS, Relation relRS){
		int numIO=0;
		
		//Insert your code here!
		
		return numIO;
	}
	
	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory to produce the result relation relRS
	 * @param relR is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	
	public int refinedSortMergeJoinRelations(Relation relR, Relation relS, Relation relRS){
		int numIO=0;
		
		//Insert your code here!
		
		return numIO;
	}

	
	
	/**
	 * Example usage of classes. 
	 */
	public static void examples(){

		/*Populate relations*/
		System.out.println("---------Populating two relations----------");
		Relation relR=new Relation("RelR");
		int numTuples=relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains "+numTuples+" tuples.");
		Relation relS=new Relation("RelS");
		numTuples=relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains "+numTuples+" tuples.");
		System.out.println("---------Finish populating relations----------\n\n");
//			
//		/*Print the relation */
//		System.out.println("---------Printing relations----------");
//		relR.printRelation(true, true);
//		relS.printRelation(true, false);
//		System.out.println("---------Finish printing relations----------\n\n");
//		
//		
//		/*Example use of RelationLoader*/
//		System.out.println("---------Loading relation RelR using RelationLoader----------");
//		RelationLoader rLoader=relR.getRelationLoader();		
//		while(rLoader.hasNextBlock()){
//			System.out.println("--->Load at most 7 blocks each time into memory...");
//			Block[] blocks=rLoader.loadNextBlocks(7);
//			//print out loaded blocks 
//			for(Block b:blocks){
//				if(b!=null) b.print(false);
//			}
//		}
//		System.out.println("---------Finish loading relation RelR----------\n\n");
//				
//		
//		/*Example use of RelationWriter*/
//		System.out.println("---------Writing to relation RelS----------");
//		RelationWriter sWriter=relS.getRelationWriter();
//		rLoader.reset();
//		if(rLoader.hasNextBlock()){
//			System.out.println("Writing the first 7 blocks from RelR to RelS");
//			System.out.println("--------Before writing-------");
//			relR.printRelation(false, false);
//			relS.printRelation(false, false);
//			
//			Block[] blocks=rLoader.loadNextBlocks(7);
//			for(Block b:blocks){
//				if(b!=null) sWriter.writeBlock(b);
//			}
//			System.out.println("--------After writing-------");
//			relR.printRelation(false, false);
//			relS.printRelation(false, false);
//		}
                mergeSortRelation(relR);

	}
	
	/**
	 * Testing cases. 
	 */
	public static void testCases(){
	
		// Insert your test cases here!
	
	}
	
	/**
	 * This main method provided for testing purpose
	 * @param arg
	 */
	public static void main(String[] arg){
		Algorithms.examples();
	}
}
