import MySQLdb as mdb
import sys
import re
import os
import time
from sets import Set
import nltk.classify.util
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import movie_reviews

#################################################################################
#
# Initializes the MySQL database connection
#
#################################################################################

def initialize_db_connection():
    con = None
    try:
        con = mdb.connect('localhost', 'root', '', 'TwitterTemp');
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
    cursor = con.cursor()
	
	#Deletes Tables in database if they exist
    cursor.execute("DROP TABLE IF EXISTS UserIDs")
    cursor.execute("DROP TABLE IF EXISTS Tweets")

	#Creates Tables in database
    cursor.execute("""
        CREATE TABLE UserIDs
        (
        table_id int NOT NULL AUTO_INCREMENT,
        PRIMARY KEY(table_id),
        Username varchar(150) NOT NULL UNIQUE,
        Tweets int UNSIGNED NOT NULL DEFAULT '0',
        Retweets int UNSIGNED NOT NULL DEFAULT '0'
        ) AUTO_INCREMENT = 1;
        """)
    cursor.execute("""
        CREATE TABLE Tweets
        (
        table_id int NOT NULL AUTO_INCREMENT,
        PRIMARY KEY(table_id),
        Tweet varchar(500),
		userID int,
		date DATE,
		time TIME,
        isMention int,
        mood int
        ) AUTO_INCREMENT = 1;
        """)
    cursor.close()

    return con

#################################################################################
#
# Runs the main program
#
#################################################################################
	
def main(argv):
    print time.asctime(time.localtime(time.time()))
    conn = initialize_db_connection()
    classifier = initialize_classifier()
    filename = argv[0] #Usage, argv[0] should be input filename
    try:
        f = open(filename, 'r')
    except IOError:
        print "Error: No Such File"
        sys.exit(1)
    else:
		#reads total number of tweets in file
        tweet_count = f.readline()[len('total number:'):]
        parse_file(f, conn, classifier)
    conn.commit()
    conn.close()
    print "sorting edges"
    sort_edges()
    print "condensing edges"
    condense_edges()
    print "building .net file"
    build_net_file()
    print time.asctime(time.localtime(time.time()))
    
def build_net_file():
    f_users = open('users_sorted.txt', 'r')
    f_edges = open('edges_sorted_condensed.txt', 'r')
    try:
        os.remove('script.net')
    except:
        0 == 0
    users = f_users.readlines()
    edges = f_edges.readlines()
    f_users.close()
    f_edges.close()
    f_net = open('script.net', 'w')
    numUsers = len(users)
    
    f_net.write('*Vertices %d\n' % (numUsers,))
    count = 1
    name_dict = {}
    for name in users:
        if name.strip() == "":
            0 == 0
        else:
            name_dict[name.strip()] = count
            f_net.write('   %d   "%s"\n' % (count, name.strip()))
            count += 1
    f_net.write('*Arcs\n')
    for edge in edges:
        if edge.strip() == "":
            0 == 0
        else:
            tokens = edge.strip().split('\t')
            f_net.write('   %s   %s   %s\n' % (name_dict[tokens[0]], name_dict[tokens[1]], tokens[2]))
    f_net.write('*Edges')
    f_net.close()
    
    
    
    
def sort_users():
    f = open('users.txt', 'r')
    try:
        os.remove('users_sorted.txt')
    except:
        0 == 0
    f_sorted = open('users_sorted.txt', 'w')
    lines = f.readlines()
    f.close()
    lines.sort()
    f_sorted.writelines(lines)
    f_sorted.close()
    os.remove('users.txt')

#################################################################################
#
# Takes the edges in 'edges.txt' and sorts them, outputting to 'edges_sorted.txt'
#
#################################################################################	
	
def sort_edges():
    f = open('edges.txt', 'r')
    try:
        os.remove('edges_sorted.txt')
    except:
        0 == 0
    f_sorted = open('edges_sorted.txt', 'w')
    lines = f.readlines()
    f.close()
    lines.sort()
    f_sorted.writelines(lines)
    f_sorted.close()
    os.remove('edges.txt')

def condense_edges():
    f = open('edges_sorted.txt', 'r')
    try:
        os.remove('edges_sorted_condensed.txt')
    except:
        0 == 0
    f_condensed = open('edges_sorted_condensed.txt', 'w')
    sentiment = 0
    line = f.readline()
    while(line):
        tokenized = line.split('\t')
        sentiment = int(tokenized[2].strip())
        next = f.readline()
        if next:
            next_tokenized = next.split('\t')
            while((tokenized[0] == next_tokenized[0]) & (tokenized[1] == next_tokenized[1])):
                sentiment += int(next_tokenized[2].strip())
                next = f.readline()
                next_tokenized = next.split('\t')
            f_condensed.write(tokenized[0] + '\t' + tokenized[1] + '\t' + str(sentiment) + '\n')
        line = next
    f.close()
    f_condensed.close()
    #os.remove('edges_sorted.txt')

def word_feats(words):
    return dict([(word, True) for word in words])
    
def make_dict(tweet):
    tweet_words = tweet.split()
    return dict([(word, True) for word in tweet_words])

def initialize_classifier():

    negids = movie_reviews.fileids('neg')
    posids = movie_reviews.fileids('pos')

    negfeats = [(word_feats(movie_reviews.words(fileids=[f])), 'neg') for f in negids]
    posfeats = [(word_feats(movie_reviews.words(fileids=[f])), 'pos') for f in posids]

    negcutoff = int(len(negfeats) * 0.50)
    poscutoff = int(len(posfeats) * 0.50)

    trainfeats = negfeats[:negcutoff] + posfeats[:poscutoff]

    classifier = NaiveBayesClassifier.train(trainfeats)
    
    #classifier.show_most_informative_features(30)
    
    return classifier

#    print 'accuracy:', nltk.classify.util.accuracy(classifier, testfeats)
#    classifier.show_most_informative_features()

#################################################################################
#
# Parses the input file and extracts relevant data.  Pulls Usernames, Tweets,
# Mentions, Times, etc and inserts them into the database
#
#################################################################################	
	
def parse_file(f, conn, classifier):
    #prepares *.txt files to assist processing data
    count = 0
    try:
        os.remove('edges.txt')
    except:
        0 == 0
    try:
        os.remove('users.txt')
    except:
        0 == 0
    f_users = open('users.txt', 'w')
    users = Set()
    f_out = open('edges.txt', 'w')
    #f_sent = open('sentiment.txt', 'w')
    cursor = conn.cursor()
    temp = f.readline()
    tracker = 0

#    while(temp != ''): # to run for entire file
    while(tracker != 5000): # to run for set number of tweets
#        print tracker               
        if(temp[0] == 'T'):
            T_line = temp
            U_line = f.readline()
            W_line = f.readline()

            date = T_line[2:12]
            time = T_line[13:]
            name = U_line[len('U\thttp://twitter.com/'):].strip().rstrip(',.:;')
            name = name.lower().strip()
            tweet = W_line[2:].strip()
            #print tweet
            cursor.execute("""
                INSERT IGNORE INTO UserIDs(Username) VALUE(%s)
                """, name)
            #commented out because we only want users involved in mentions, not all
            #users.add(name)
            cursor.execute("""
                INSERT IGNORE INTO Tweets(Tweet, date, time, isMention) VALUE(%s, %s, %s, 0)
                """, (tweet, date, time))
            cursor.execute("""
                UPDATE UserIDs SET Tweets=Tweets+1 WHERE Username=%s;
                """, name)
            atIndex = tweet.find('@')
            if(atIndex > -1):
                tweetWords = tweet.split()
                for word in tweetWords:
                    word = word.strip(',.;:)(*#')
                    k = word.find('@')
                    if((k == 0) & (len(word) > 1)):
                        target = word[1:].rstrip(',.;:)(*#')
                        target = target.lower()
                        cursor.execute("""
                            INSERT IGNORE INTO UserIDs(Username) Value(%s)
                            """, target)
                        users.add(target)
                        users.add(name)
                        tracker += 1
                        cursor.execute("""
                            UPDATE UserIDs SET Retweets=Retweets+1 WHERE Username=%s;
                            """, name)
                        cursor.execute("""
                            UPDATE Tweets SET isMention = 1 WHERE Tweet = %s
                            """, tweet)

                        sentiment = classifier.classify(make_dict(tweet))
                        if sentiment == 'pos':
                            sent = 1
                        else:
                            sent = -1
                        
                        cursor.execute("""
                            UPDATE Tweets SET mood = %s WHERE Tweet = %s
                            """, (sent, tweet))
                        f_out.write(name+'\t'+target+'\t'+str(sent)+'\n')
                        #f_sent.write(name+'\t'+target+'\t'+sentiment+'\t\n')
        #tracker = tracker + 1
        temp = f.readline()
    for line in users:
        f_users.write(line+'\n')
    f_users.close()
    sort_users()
    cursor.close()
        
if __name__ == "__main__":
    main(sys.argv[1:])
    
