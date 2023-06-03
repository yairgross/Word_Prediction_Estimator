Submitters:
	Sharon Hendy 209467158
	Yair Gross 314625781
	
	
Instructions to run our project:
1. Create maven projects and jars for each src folder of each Map-Reduce step, and for the Main program which is responsible for running them.
2. Create a bucket in S3 and upload those jars.
3. For the Main program jar, run the following command:
	java -jar main-jar-with-dependencies.jar <bucket-name-in-S3>


We ran our project on the British English corpus.
Our project was composed of 3 different Map-Reduce jobs:
1. OccurrencesCounter:
	- Map function: Reads each line from the corpus (while filtering stop words), and writes for each line:
		The 3-gram;
		The 3-gram's number of occurrences;
		To which part (first / second) of the corpus it belongs.
	- Reduce function: For each 3-gram, aggregates the total number of occurrences and writes to the context:
		The 3-gram;
		Total occurrences in first half of the corpus;
		Total occurrences in second half of the corpus;
		Total occurrences in the entire corpus;
2. PdelCalculator:
	- Map function: For each number r of occurrences, writes to the context:
		If r is a number representing occurrences of a specific 3-gram in the entire corpus, sends r with the corresponding 3-gram;
		If r is a number representing occurrences of a specific 3-gram in the first / second part of the corpus, sends r with the number of occurrences of the corresponding 3-gram in the opposite half of the corpus.
	- Reduce function: For each number r of occurrences in the entire corpus:
		Saves all the 3-grams appearing r times;
		(Note: the reducer saves the 3-grams for a specific r in the memory, and cleans the memory when r changes, thus not saving all the 3-grams for every r in the memory).
		Aggregates the numbers needed for the probability calculation;
		Calculated the deleted estimation probability for each 3-gram occurring r times in the corpus;
		Writes to the context the 3-gram along with it's corresponding probability.
3. SortNgram:
	- Map function: For every pair of 3-gram w1w2w3 and probability p:
		Sends a key composed of w1w2 and p, and value w3.
	- A unique compareTo function was implemented, sorting by w1w2 in increasing order and p in decreasing order.
	- Reduce function: for every key-value pair (<w1w2, p>, w3):
		Writes to the context the 3-gram w1w2w3 with the probability p.
		
		
Statistics:
	For the OccurrencesCounter step:
		- Number of output records from mapper: 38400722
		- Size (in bytes) of output records from mapper: 1261267255
	For the PdelCalculator step:
		- Number of output records from mapper: 2673177
		- Size (in bytes) of output records from mapper: 45193912
	For the SortNgram step:
		- Number of output records from mapper: 891059
		- Size (in bytes) of output records from mapper: 23575281

Analysis:
	Some interesting word pairs and their next words (the first word is with the highest probability):
		- "acorded special": "privileges", "treatment", "status", "attention", "protection"
		- "accordingly gave": "orders", "notice", "way", "judgment", "instructions"
		- "block diagram": "form", "shown", "representation", "showing", "shows"
		- "blood cells": "counts", "production", "formation", "mass", "membrane"
		- "common people": "heard", "did", "say", "called", "know"
		- "delicate blue": "colour", "veins", "flowers", "sky", "tint"
		- "felt quite": "certain", "sorry", "happy", "safe", "ashamed"
		- "health care": "delivery", "costs", "facilities", "setting", "policy"
		- "probably best": "known", "avoided", "understood", "left", "regarded"
		- "racially motivated": "crime", "violence", "incidents", "attacks", "crimes"