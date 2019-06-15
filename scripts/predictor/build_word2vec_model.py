import psycopg2
import numpy as np
from gensim.models import Word2Vec
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer



conn_string = "host='localhost' dbname='referendum_catalonia_db' user='postgres' password='########'"

users_table = "processed_users"
tweets_table = "tweets_metadata"


select_all_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + users_table + " as u ON t.userid = u.userid"


model_path = '/data/project/catalonia/model/model'
model_txt_path = '/data/project/catalonia/model/model.txt'

model_nonstop_path = '/data/project/catalonia/model/model_nonstop'
model_nonstop_txt_path = '/data/project/catalonia/model/model_nonstop.txt'



def main():	
	whole_corpus_raw = build_corpus(select_all_tweets_query)
	whole_corpus = []

	whole_corpus = []
	# pre-process the corpus
	for tweet in whole_corpus_raw:
		whole_corpus.append(apply_preprocessing_to_corpus(tweet))

	whole_corpus_size = len(whole_corpus)
	model = train_word2vec_model(whole_corpus, whole_corpus_size)
	model.wv.save_word2vec_format(model_txt_path, binary=False)

	# corpus_vector = model.wv.syn0
	# print(type(corpus_vector))
	# print(corpus_vector.shape)







def train_word2vec_model(corpus, corpus_size):
	model = Word2Vec(min_count=1)
	model.build_vocab(corpus)
	model.train(corpus, total_examples=model.corpus_count, epochs=model.iter)
	model.save(model_path)
	# calling the train() method sets the number of features to a default 100	

	return model



def apply_preprocessing_to_corpus(tweet):
	tokenizer = TweetTokenizer()
	# tokenize tweet text
	tweet_tokenized = tokenizer.tokenize(' '.join(tweet))
	# remove the URLs 
	tweet_url = [w for w in tweet_tokenized if 'http' not in w.lower()]
	# remove punctuation
	tweet_punct = [remove_extra_chars_from_word(w) for w in tweet_url]
	# remove stop-words	
	# stop_words = set(stopwords.words('italian'))
	stop_words = set(stopwords.words('spanish'))
	tweet = [w for w in tweet_punct if w.lower() not in stop_words]
	# remove any '' word as result of preceding processing
	tweet = [w for w in tweet if w != '']

	return tweet



def remove_extra_chars_from_word(word):	
	word = word.replace('.', '')
	word = word.replace(',', '')
	word = word.replace(';', '')
	word = word.replace(':', '')
	word = word.replace('"', '')
	word = word.replace("'", '')
	word = word.replace('/', '')
	word = word.replace('!', '')
	word = word.replace('?', '')
	word = word.replace('-', '')
	word = word.replace('_', '')
	word = word.replace('(', '')
	word = word.replace(')', '')
	word = word.replace('[', '')
	word = word.replace(']', '')
	word = word.replace('{', '')
	word = word.replace('}', '')	
	word = word.replace('^', '')
	word = word.replace('*', '')
	word = word.replace('+', '')
	word = word.replace('+', '')
	word = word.replace('«', '')
	word = word.replace('»', '')	
	word = word.replace('…', '')

	return word



def build_corpus(select_tweets_query):
	corpus_raw = []
	
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_tweets_query)
		rows = cur.fetchall()			
		
		for row in rows:			
			text = row[0]
			words = [word for word in text.split()]			
			# need a list of tweets text (list of list)
			corpus_raw.append(words)
		
		cur.close()
		conn.close()		
	except psycopg2.Error as e:
		print(e.pgerror)
		print("Connection failed")

	return corpus_raw









main()