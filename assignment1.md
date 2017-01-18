Assignment 1
==================

###Question 1.
* Pairs: There are 2 MapReduce jobs.
  * The first mapper is to go through each line and count every word once. It also count the total number of lines and store it in a Counter. The output is (Text word, IntWritable 1) for every unique word.
  * The first reducer is to sum up all the values from the first mapper and emit (Text word, IntWritable sum) pair for every unique word. The output is store in a temporary file in local disk.
  * The second mapper is to go through each line and count every unique pair of words occur in the line. The output is (PairOfStrings (word1, word2), IntWritable 1).
  * The second combiner/reducer is to sum up all the output from the second mapper and load the side data stored during the first job. The reducer use these information to calculate the PMI and co-occurrence count of each pair of words. The final output is (PairOfStrings (word1, word2), PairOfFloatInt (PMI, count))
  
* Stripes: There are 2 MapReduce jobs with the first job the same as Pairs implementation
  * The first mapper is to go through each line and count every word once. It also count the total number of lines and store it in a Counter. The output is (Text word, IntWritable 1) for every unique word.
  * The first reducer is to sum up all the values from the first mapper and emit (Text word, IntWritable sum) pair for every unique word. The output is store in a temporary file in local disk.
  * The second mapper is to go through each line and emit a key-value pair with each word as the key and the value being a map in the form of (word, {word1 : 1, word2 : 1 ...}).
  * The second combiner sums up all the values for the same key of the hashmap emitted from the second mapper.
  * The second reducer is also to sum up all the values for the same key of the hashmap emitted from the combiner and load the side data stored during the first job as well as total line number. The reducer then use these information to calculate the PMI and co-occurrence count of each pair of words. The final output is emitted as (word, {word1 : (PMI, count), word2 : (PMI, count)...})

###Question 2.
Ran on linux.student.cs.uwaterloo.ca
* Pairs:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 5.007 + 35.667 = 40.674 seconds
* Stripes:&nbsp;&nbsp; 4.937 + 11.579 = 16.516 seconds

###Question 3.
Ran on linux.student.cs.uwaterloo.ca (disable all combiners)
* Pairs:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 5.973 + 38.642 = 44.615 seconds
* Stripes:&nbsp;&nbsp; 5.954 + 12.582 = 18.536 seconds

###Question 4.
77198 &nbsp;&nbsp; 308792 &nbsp;&nbsp; 2327632

###Question 5. (highest PMI)
* (maine, anjou) &nbsp;&nbsp; (3.6331422, 12)
* (anjou, maine) &nbsp;&nbsp; (3.6331422, 12)

###Question 5. (lowest PMI)
* (thy, you) &nbsp;&nbsp; (-1.5303968, 11)
* (you, thy) &nbsp;&nbsp; (-1.5303968, 11)

###Question 6. ('tears')
* (tears, shed) &nbsp;&nbsp; (2.1117902, 15)
* (tears, salt) &nbsp;&nbsp;&nbsp; (2.052812, 11)
* (tears, eyes) &nbsp;&nbsp; (1.165167, 23)

###Question 6. ('death')
* (death, father's) &nbsp;&nbsp; (1.120252, 21)
* (death, die) &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; (0.7541594, 18)
* (death, life)	&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; (0.73813456, 31)

###Question 7.
* (hockey, defenceman) &nbsp; (2.4030268, 147)
* (hockey, winger) &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; (2.3863757, 185)
* (hockey, goaltender) &nbsp;&nbsp;&nbsp; (2.2434428, 198)
* (hockey, ice) &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; (2.195185, 2002)
* (hockey, nhl)	&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; (1.9864639, 940)

###Question 8.
* (data, storage) &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; (1.9796829, 100)
* (data, database) &nbsp;&nbsp;&nbsp; (1.8992721, 97)
* (data, disk) &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; (1.7935462, 67)
* (data, stored) &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; (1.7868549, 65)
* (data, processing) &nbsp; (1.6476576, 57)
