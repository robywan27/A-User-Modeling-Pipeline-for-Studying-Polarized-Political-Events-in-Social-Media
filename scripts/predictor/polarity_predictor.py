import psycopg2
import numpy as np
import pandas as pd
from gensim.models import Word2Vec
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer
from collections import Counter
from sklearn.preprocessing import LabelEncoder
import random
# datasets
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from imblearn.ensemble import EasyEnsemble
from sklearn.model_selection import KFold
from sklearn.model_selection import cross_validate
from sklearn.model_selection import ShuffleSplit
# classifiers
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
# evaluation
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import f1_score
from sklearn.metrics import accuracy_score
from sklearn.metrics import roc_curve
from sklearn.metrics import auc
from sklearn.metrics import roc_auc_score




conn_string = "host='localhost' dbname='referendum_catalonia_db' user='postgres' password='########'"


negative_tweets_table = "negative_tweetids"
positive_tweets_table = "positive_tweetids"
neutral_tweets_table = "neutral_tweetids"
non_relevant_table = "non_relevant_tweetids"
users_table = "processed_users"
tweets_table = "tweets_metadata"

# tweets for relevance predictor
select_non_relevant_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + non_relevant_table + " as nr ON t.tweetid = nr.tweetid LIMIT 1000"
# select_negative_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 333"
# select_positive_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 333"
# select_neutral_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 334"
select_negative_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid"
select_positive_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid"
select_neutral_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid"
# select_neutral_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + users_table + " as n ON t.userid = n.userid WHERE (lower(text) like '%catalan%referendum%' or lower(text) like '%catalona%referendum' or lower(text) like '%referendum%cataluna' or lower(text) like '%referendum%catalonya') and lower(text) not like '%vot%' and lower(text) not like '%yes%' and lower(text) not like '%!%' and lower(text) not like '%viva%' and date(t.created_at) = '2017-10-01' ORDER BY t.tweetid LIMIT 334"

# tweets for negative and positive predictors
# select_negative_predictor_negative_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 500"
# select_positive_predictor_positive_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid LIMIT 500"
# select_non_positive_negative_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 250"
# select_non_negative_positive_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 250"
# select_negpos_pred_neutral_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 250"
select_negative_predictor_negative_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + users_table + " as n ON t.userid = n.userid WHERE politicalview = 'Negative' ORDER BY t.userid LIMIT 500"
select_positive_predictor_positive_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + users_table + " as p ON t.userid = p.userid WHERE politicalview = 'Positive' ORDER BY t.userid LIMIT 500"
select_non_positive_negative_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + users_table + " as n ON t.userid = n.userid WHERE politicalview = 'Negative' ORDER BY t.userid LIMIT 250"
select_non_negative_positive_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + users_table + " as p ON t.userid = p.userid WHERE politicalview = 'Positive' ORDER BY t.userid LIMIT 250"
select_negpos_pred_neutral_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + users_table + " as n ON t.userid = n.userid WHERE (lower(text) like '%catalan%referendum%' or lower(text) like '%catalona%referendum' or lower(text) like '%referendum%cataluna' or lower(text) like '%referendum%catalonya') and lower(text) not like '%vot%' and lower(text) not like '%yes%' and lower(text) not like '%!%' and lower(text) not like '%viva%' and date(t.created_at) = '2017-10-01' ORDER BY t.tweetid LIMIT 250"


# other positive and negative tweets
select_negative_166_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 167"
select_positive_166_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 167"
select_neutral_166_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 166"
select_non_relevant_500_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + non_relevant_table + " as nr ON t.tweetid = nr.tweetid ORDER BY t.userid LIMIT 500"

select_test_negative_166_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 167"
select_test_positive_166_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 167"
select_test_neutral_166_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 166"
select_test_non_relevant_500_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + non_relevant_table + " as nr ON t.tweetid = nr.tweetid ORDER BY t.userid OFFSET 500"

select_negative_250_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 250"
select_positive_250_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 250"
select_non_positive_83_negative_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 83"
select_non_negative_83_positive_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 83"
select_negpos_pred_83_neutral_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 84"

select_test_negative_250_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 250"
select_test_positive_250_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 250"
select_test_non_positive_83_negative_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 83"
select_test_non_negative_83_positive_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 83"
select_test_negpos_pred_83_neutral_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 84"

select_negative_83_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 83"
select_positive_83_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 83"
select_neutral_83_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 84"
select_non_relevant_250_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + non_relevant_table + " as nr ON t.tweetid = nr.tweetid ORDER BY t.userid LIMIT 250"

select_test_negative_83_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 83"
select_test_positive_83_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 83"
select_test_neutral_83_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 84"
select_test_non_relevant_250_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + non_relevant_table + " as nr ON t.tweetid = nr.tweetid ORDER BY t.userid OFFSET 250"

select_negative_125_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 125"
select_positive_125_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 125"
select_non_positive_41_negative_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 42"
select_non_negative_41_positive_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 42"
select_negpos_pred_41_neutral_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 41"

select_test_negative_125_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 125"
select_test_positive_125_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 125"
select_test_non_positive_41_negative_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 42"
select_test_non_negative_41_positive_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 42"
select_test_negpos_pred_41_neutral_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 41"

select_negative_33_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 33"
select_positive_33_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 33"
select_neutral_33_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid LIMIT 34"
select_non_relevant_100_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + non_relevant_table + " as nr ON t.tweetid = nr.tweetid ORDER BY t.userid LIMIT 100"

select_test_negative_33_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 33"
select_test_positive_33_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 33"
select_test_neutral_33_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid ORDER BY t.userid OFFSET 34"
select_test_non_relevant_100_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + non_relevant_table + " as nr ON t.tweetid = nr.tweetid ORDER BY t.userid OFFSET 100"

# test with samples of non-classified tweets
select_unclassified_tweets_query = "SELECT text FROM " + tweets_table + " as t JOIN " + users_table + " as u ON t.userid = u.userid WHERE polarity is null"
select_unclassified_tweetids_query = "SELECT tweetid FROM " + tweets_table + " as t JOIN " + users_table + " as u ON t.userid = u.userid WHERE polarity is null"
select_relevant_tweets_query = "SELECT text FROM " + tweets_table + " WHERE polarity = 'Relevant'"
select_relevant_tweetids_query = "SELECT tweetid FROM " + tweets_table + " WHERE polarity = 'Relevant'"
select_users_polarized_tweets_query = "SELECT u.userid FROM " + tweets_table + " as t JOIN " + users_table + " as u ON t.userid = u.userid WHERE politicalview is null AND (polarity = 'Negative' OR polarity = 'Positive' OR polarity = 'Neutral')"
select_negposneu_pred_tweets_query = "(SELECT text, t.userid FROM " + tweets_table + " as t JOIN " + negative_tweets_table + " as ng ON t.tweetid = ng.tweetid ORDER BY t.userid LIMIT 400)" + \
								"UNION ALL" + \
								"(SELECT text, t.userid FROM " + tweets_table + " as t JOIN " + positive_tweets_table + " as p ON t.tweetid = p.tweetid ORDER BY t.userid LIMIT 400)" + \
								"UNION ALL" + \
								"(SELECT text, t.userid FROM " + tweets_table + " as t JOIN " + neutral_tweets_table + " as n ON t.tweetid = n.tweetid)"


negative_keywords_path = '../lombardia/no_keywords.txt'
positive_keywords_path = '../lombardia/yes_keywords.txt'

model_path = '/data/project/catalonia/model/model'

relevant_tweets_path = '/data/project/catalonia/tweets/relevant_words.txt'
non_relevant_tweets_path = '/data/project/catalonia/tweets/non_relevant_words.txt'
positive_tweets_path = '/data/project/catalonia/tweets/positive_words.txt'
negative_tweets_path = '/data/project/catalonia/tweets/negative_words.txt'
neutral_tweets_path = '/data/project/catalonia/tweets/neutral_words.txt'
unclassified_tweets_path = '/data/project/catalonia/tweets/unclassified_words.txt'
unclassified_relevant_tweets_path = '/data/project/catalonia/tweets/unclassified_relevant_words.txt'
relevant_preprocessed_tweets_path = '/data/project/catalonia/preprocessed_tweets/relevant_preprocessed_words.txt'
non_relevant_preprocessed_tweets_path = '/data/project/catalonia/preprocessed_tweets/non_relevant_preprocessed_words.txt'
positive_preprocessed_tweets_path = '/data/project/catalonia/preprocessed_tweets/positive_preprocessed_words.txt'
negative_preprocessed_tweets_path = '/data/project/catalonia/preprocessed_tweets/negative_preprocessed_words.txt'
neutral_preprocessed_tweets_path = '/data/project/catalonia/preprocessed_tweets/neutral_preprocessed_words.txt'
unclassified_preprocessed_tweets_path = '/data/project/catalonia/preprocessed_tweets/unclassified_preprocessed_words.txt'
unclassified_relevant_preprocessed_tweets_path = '/data/project/catalonia/preprocessed_tweets/unclassified_relevant_preprocessed_words.txt'

relevant_test_labels_path = '/data/project/catalonia/predicted_labels/relevant_test_labels.txt'
negative_test_labels_path = '/data/project/catalonia/predicted_labels/negative_test_labels.txt'
positive_test_labels_path = '/data/project/catalonia/predicted_labels/positive_test_labels.txt'
negative_second_test_labels_path = '/data/project/catalonia/predicted_labels/negative_second_test_labels.txt'
positive_second_test_labels_path = '/data/project/catalonia/predicted_labels/positive_second_test_labels.txt'
unclassified_test_labels_path = '/data/project/catalonia/predicted_labels/unclassified_test_labels.txt'
unclassified_relevant_negative_test_labels_path = '/data/project/catalonia/predicted_labels/unclassified_relevant_negative_test_labels.txt'
unclassified_relevant_positive_test_labels_path = '/data/project/catalonia/predicted_labels/unclassified_relevant_positive_test_labels.txt'

average_vector_path = '/data/project/catalonia/average_vector.txt'


negative_keywords = []
positive_keywords = []

relevant_label_string = 'Relevant'
non_relevant_label_string = 'Non Relevant'
negative_label_string = 'Negative'
non_negative_label_string = 'Non Negative'
positive_label_string = 'Positive'
non_positive_label_string = 'Non Positive'

possibly_positive_string = 'PredictedPositive'
possibly_negative_string = 'PredictedNegative'



# load the word2vec model of the whole tweets corpus
model = Word2Vec.load(model_path)
# print(model["lombardia"])






def experiment_driver():
	# create a list for negative keywords and one list for positive keywords
	prepare_keywords()
	
	################################################################################################################
	# Step 1. Build a RELEVANT/NON-RELEVANT PREDICTOR
	# TRAINING PHASE: feed the predictor with relevant tweets (positive and negative) and non-relevant tweets
	# TEST PHASE: test the predictor with a sample of positive and negative tweets
	# RESULTS: calculate PRECISION, RECALL, F1-MEASURE (others?) from the results of the test phase
	################################################################################################################

	# build sets for relevant (positive + negative + neutral) and non-relevant corpus
	print("Relevant - non relevant")
	# build relevant corpus
	relevant_corpus = []	
	
	negative_corpus = build_corpus(select_negative_tweets_query, negative_tweets_path, negative_preprocessed_tweets_path)
	relevant_corpus.extend(negative_corpus)	
	
	positive_corpus = build_corpus(select_positive_tweets_query, positive_tweets_path, positive_preprocessed_tweets_path)
	relevant_corpus.extend(positive_corpus)	
	
	neutral_corpus = build_corpus(select_neutral_tweets_query, neutral_tweets_path, neutral_preprocessed_tweets_path)
	relevant_corpus.extend(neutral_corpus)	
	
	# build non relevant corpus
	non_relevant_corpus = build_corpus(select_non_relevant_tweets_query, non_relevant_tweets_path, non_relevant_preprocessed_tweets_path)
	
	# get 2 vectors of features and labels
	features, labels = build_features_labels(relevant_corpus, non_relevant_corpus, relevant_label_string, non_relevant_label_string)

	# train, test and evaluate a redictor, which is returned
	relevance_predictor = build_train_test_evaluate_predictor(features, labels, 1)	

	
	################################################################################################################
	# Step 2. Build a POSITIVE/NON-POSITIVE PREDICTOR and a NEGATIVE/NON-NEGATIVE PREDICTOR
	# LEGEND: P = {positive tweets}, N = {negative tweets}, NE = {neutral tweets}, R = {relevant tweets, t| t∈P V t∈N V t∈NE}
	# TRAINING PHASE: feed both predictors only with the relevant tweets
	# 	feed the positive predictor with tweets t| t∈P V t∈(R\P)
	# 	feed the negative predictor with tweets t| t∈N V t∈(R\N)
	# TEST PHASE: test the predictors with a sample of relevant tweets (positive, negative, neutral)
	# RESULTS: calculate PRECISION, RECALL, F1-MEASURE (others?) from the results of the test phase
	################################################################################################################

	# Build negative predictor
	# build sets	
	print("Negative")
	negative_corpus = build_corpus(select_negative_predictor_negative_tweets_query, negative_tweets_path, negative_preprocessed_tweets_path)
	non_negative_corpus = []
	
	positive_corpus = build_corpus(select_non_negative_positive_tweets_query, positive_tweets_path, positive_preprocessed_tweets_path)
	non_negative_corpus.extend(positive_corpus)	
	
	neutral_corpus = build_corpus(select_negpos_pred_neutral_tweets_query, neutral_tweets_path, neutral_preprocessed_tweets_path)
	non_negative_corpus.extend(neutral_corpus)	
	
	features, labels = build_features_labels(negative_corpus, non_negative_corpus, negative_label_string, non_negative_label_string)	
	negative_predictor = build_train_test_evaluate_predictor(features, labels, 0)


	# Build positive predictor
	# build sets	
	print("Positive")
	positive_corpus = build_corpus(select_positive_predictor_positive_tweets_query, positive_tweets_path, positive_preprocessed_tweets_path)
	non_positive_corpus = []
	
	negative_corpus = build_corpus(select_non_positive_negative_tweets_query, negative_tweets_path, negative_preprocessed_tweets_path)
	non_positive_corpus.extend(negative_corpus)	
	
	neutral_corpus = build_corpus(select_negpos_pred_neutral_tweets_query, neutral_tweets_path, neutral_preprocessed_tweets_path)
	non_positive_corpus.extend(neutral_corpus)	
	
	features, labels = build_features_labels(positive_corpus, non_positive_corpus, positive_label_string, non_positive_label_string)	
	positive_predictor = build_train_test_evaluate_predictor(features, labels, 1)



	# Half dataset considered	
	print()
	print()
	print("Half dataset")

	# test relevance predictor with 500 tweets
	print("Relevant - non relevant")
	# build relevant corpus
	relevant_corpus = []	
	
	negative_corpus = build_corpus_with_keywords(select_negative_166_tweets_query, negative_keywords, negative_tweets_path, negative_preprocessed_tweets_path)
	relevant_corpus.extend(negative_corpus)	
	
	positive_corpus = build_corpus_with_keywords(select_positive_166_tweets_query, positive_keywords, positive_tweets_path, positive_preprocessed_tweets_path)
	relevant_corpus.extend(positive_corpus)	
	
	neutral_corpus = build_corpus(select_neutral_166_tweets_query, neutral_tweets_path, neutral_preprocessed_tweets_path)
	relevant_corpus.extend(neutral_corpus)	
	
	# build non relevant corpus
	non_relevant_corpus = build_corpus(select_non_relevant_500_tweets_query, non_relevant_tweets_path, non_relevant_preprocessed_tweets_path)
	
	# get 2 vectors of features and labels
	features, labels = build_features_labels(relevant_corpus, non_relevant_corpus, relevant_label_string, non_relevant_label_string)

	# train, test and evaluate a redictor, which is returned
	relevance_predictor = build_train_test_evaluate_predictor(features, labels)


	# build sets	
	print("Negative")
	negative_corpus = build_corpus_with_keywords(select_negative_250_tweets_query, negative_keywords, negative_tweets_path, negative_preprocessed_tweets_path)
	non_negative_corpus = []
	
	positive_corpus = build_corpus_with_keywords(select_non_negative_83_positive_tweets_query, positive_keywords, positive_tweets_path, positive_preprocessed_tweets_path)
	non_negative_corpus.extend(positive_corpus)	
	
	neutral_corpus = build_corpus(select_negpos_pred_83_neutral_tweets_query, neutral_tweets_path, neutral_preprocessed_tweets_path)
	non_negative_corpus.extend(neutral_corpus)	
	
	features, labels = build_features_labels(negative_corpus, non_negative_corpus, negative_label_string, non_negative_label_string)	
	negative_predictor = build_train_test_evaluate_predictor(features, labels)

	
	# build sets	
	print("Positive")
	positive_corpus = build_corpus_with_keywords(select_positive_250_tweets_query, positive_keywords, positive_tweets_path, positive_preprocessed_tweets_path)
	non_positive_corpus = []
	
	negative_corpus = build_corpus_with_keywords(select_non_positive_83_negative_tweets_query, negative_keywords, negative_tweets_path, negative_preprocessed_tweets_path)
	non_positive_corpus.extend(negative_corpus)	
	
	neutral_corpus = build_corpus(select_negpos_pred_83_neutral_tweets_query, neutral_tweets_path, neutral_preprocessed_tweets_path)
	non_positive_corpus.extend(neutral_corpus)	
	
	features, labels = build_features_labels(positive_corpus, non_positive_corpus, positive_label_string, non_positive_label_string)	
	positive_predictor = build_train_test_evaluate_predictor(features, labels)



	# A quarter dataset considered
	print()
	print()
	print("A quarter dataset")

	# test relevance predictor with 500 tweets
	print("Relevant - non relevant")
	# build relevant corpus
	relevant_corpus = []	
	
	negative_corpus = build_corpus_with_keywords(select_negative_83_tweets_query, negative_keywords, negative_tweets_path, negative_preprocessed_tweets_path)
	relevant_corpus.extend(negative_corpus)	
	
	positive_corpus = build_corpus_with_keywords(select_positive_83_tweets_query, positive_keywords, positive_tweets_path, positive_preprocessed_tweets_path)
	relevant_corpus.extend(positive_corpus)	
	
	neutral_corpus = build_corpus(select_neutral_83_tweets_query, neutral_tweets_path, neutral_preprocessed_tweets_path)
	relevant_corpus.extend(neutral_corpus)	
	
	# build non relevant corpus
	non_relevant_corpus = build_corpus(select_non_relevant_250_tweets_query, non_relevant_tweets_path, non_relevant_preprocessed_tweets_path)
	
	# get 2 vectors of features and labels
	features, labels = build_features_labels(relevant_corpus, non_relevant_corpus, relevant_label_string, non_relevant_label_string)

	# train, test and evaluate a redictor, which is returned
	relevance_predictor = build_train_test_evaluate_predictor(features, labels)	


	# build sets	
	print("Negative")
	negative_corpus = build_corpus_with_keywords(select_negative_125_tweets_query, negative_keywords, negative_tweets_path, negative_preprocessed_tweets_path)
	non_negative_corpus = []
	
	positive_corpus = build_corpus_with_keywords(select_non_negative_41_positive_tweets_query, positive_keywords, positive_tweets_path, positive_preprocessed_tweets_path)
	non_negative_corpus.extend(positive_corpus)	
	
	neutral_corpus = build_corpus(select_negpos_pred_41_neutral_tweets_query, neutral_tweets_path, neutral_preprocessed_tweets_path)
	non_negative_corpus.extend(neutral_corpus)	
	
	features, labels = build_features_labels(negative_corpus, non_negative_corpus, negative_label_string, non_negative_label_string)	
	negative_predictor = build_train_test_evaluate_predictor(features, labels)

	
	# build sets	
	print("Positive")
	positive_corpus = build_corpus_with_keywords(select_positive_125_tweets_query, positive_keywords, positive_tweets_path, positive_preprocessed_tweets_path)
	non_positive_corpus = []
	
	negative_corpus = build_corpus_with_keywords(select_non_positive_41_negative_tweets_query, negative_keywords, negative_tweets_path, negative_preprocessed_tweets_path)
	non_positive_corpus.extend(negative_corpus)	
	
	neutral_corpus = build_corpus(select_negpos_pred_41_neutral_tweets_query, neutral_tweets_path, neutral_preprocessed_tweets_path)
	non_positive_corpus.extend(neutral_corpus)	
	
	features, labels = build_features_labels(positive_corpus, non_positive_corpus, positive_label_string, non_positive_label_string)	
	positive_predictor = build_train_test_evaluate_predictor(features, labels)



	# One tenth
	print()
	print()
	print("One tenth dataset")

	# test relevance predictor with 500 tweets
	print("Relevant - non relevant")
	# build relevant corpus
	relevant_corpus = []	
	
	negative_corpus = build_corpus_with_keywords(select_negative_33_tweets_query, negative_keywords, negative_tweets_path, negative_preprocessed_tweets_path)
	relevant_corpus.extend(negative_corpus)	
	
	positive_corpus = build_corpus_with_keywords(select_positive_33_tweets_query, positive_keywords, positive_tweets_path, positive_preprocessed_tweets_path)
	relevant_corpus.extend(positive_corpus)	
	
	neutral_corpus = build_corpus(select_neutral_33_tweets_query, neutral_tweets_path, neutral_preprocessed_tweets_path)
	relevant_corpus.extend(neutral_corpus)	
	
	# build non relevant corpus
	non_relevant_corpus = build_corpus(select_non_relevant_100_tweets_query, non_relevant_tweets_path, non_relevant_preprocessed_tweets_path)
	
	# get 2 vectors of features and labels
	features, labels = build_features_labels(relevant_corpus, non_relevant_corpus, relevant_label_string, non_relevant_label_string)

	# train, test and evaluate a redictor, which is returned
	relevance_predictor = build_train_test_evaluate_predictor(features, labels)





	###############################################################################################################
	# Application: apply the predictors on the unclassified tweets
	###############################################################################################################
	# relevance test
	unclassified_df = build_unclassified_data(select_unclassified_tweets_query, select_unclassified_tweetids_query)	
	apply_predictor_to_unclassified_data(relevance_predictor, unclassified_df)

	# positive and negative predictors in parallel
	relevant_df = build_unclassified_data(select_relevant_tweets_query, select_relevant_tweetids_query)
	apply_polarity_predictor_to_relevant_data(negative_predictor, positive_predictor, relevant_df)



	###############################################################################################################
	# Application: update users alignment
	###############################################################################################################	
	update_users_political_alignment()
	


	###############################################################################################################
	# Test: one three-class predictor for positive, negative, neutral
	###############################################################################################################	
	keywords = []
	keywords.extend(negative_keywords)
	keywords.extend(positive_keywords)	
	negposneu_corpus = build_corpus_with_keywords(select_negposneu_pred_tweets_query, keywords, relevant_tweets_path, relevant_preprocessed_tweets_path)	
	
	features, labels = build_features_multinomial_labels(negposneu_corpus, positive_label_string, negative_label_string, neutral_label_string)	
	build_train_test_evaluate_multinomial_predictor(features, labels, 0)
	build_train_test_evaluate_multinomial_predictor(features, labels, 2)
	build_train_test_evaluate_multinomial_predictor(features, labels, 1)



	


def apply_predictor_to_unclassified_data(predictor, test_vocabulary, test_labels_path):
	print()
	print()	
	# the model was defined with default 100 features
	num_features = 100	

	# extract trained vector representation of the training sets
	# print("Creating average feature vecs for the corpuses.......")
	test_feature_vector = get_average_feature_vecs(test_vocabulary, model, num_features)

	# Test phase predictor
	predicted_label_vector = predictor.predict(test_feature_vector)
	# print('Writing to file predicted labels for the test data....')
	with open(test_labels_path, 'w') as f:
		f.write(str(predicted_label_vector))





# ###############################################################################################################
# Training/Test: build the three predictors
# ###############################################################################################################

def build_train_test_evaluate_predictor(features, labels, label_string):
# def build_train_test_evaluate_predictor():
	training_precisions = []
	training_recalls = []
	training_f_measures = []
	training_accuracies = []
	training_auroc_scores = []
	test_precisions = []
	test_recalls = []
	test_f_measures = []
	test_accuracies = []	
	test_auroc_scores = []	

	# label_string = 0

	for i in range(10):
		# non_negative_corpus = []
	
		# positive_corpus = build_corpus_with_keywords(select_positive_tweets_query, positive_keywords, positive_tweets_path, positive_preprocessed_tweets_path)
		# positive_random_sample = select_random_sample(positive_corpus, 250)
		# non_negative_corpus.extend(positive_random_sample)
		
		# neutral_corpus = build_corpus(select_neutral_tweets_query, neutral_tweets_path, neutral_preprocessed_tweets_path)
		# neutral_random_sample = select_random_sample(neutral_corpus, 250)
		# non_negative_corpus.extend(neutral_random_sample)	

		# # build negative corpus
		# negative_corpus = []
		# negative_corpus = build_corpus_with_keywords(select_negative_tweets_query, negative_keywords, negative_tweets_path, negative_preprocessed_tweets_path)
		# negative_corpus.extend(select_random_sample(negative_corpus, 500))

		
		# features, labels = build_features_labels(negative_corpus, non_negative_corpus, negative_label_string, non_negative_label_string)


		training_features, test_features, training_labels, test_labels = build_training_test_sets(features, labels)		
		predictor = train_predictor(training_features, training_labels)

		predicted_labels = predictor.predict(training_features)
		predicted_probabilities = predictor.predict_proba(training_features)
		precision, recall, f_measure, auroc_score = evaluate_performance_predictor(training_labels, predicted_labels, predicted_probabilities[:,0], label_string)	
		accuracy = predictor.score(training_features, training_labels)

		training_precisions.append(precision)
		training_recalls.append(recall)
		training_f_measures.append(f_measure)
		training_accuracies.append(accuracy)
		training_auroc_scores.append(auroc_score)
		
		predicted_labels = predictor.predict(test_features)
		predicted_probabilities = predictor.predict_proba(test_features)
		precision, recall, f_measure, auroc_score = evaluate_performance_predictor(test_labels, predicted_labels, predicted_probabilities[:,0], label_string)	
		accuracy = predictor.score(test_features, test_labels)		

		test_precisions.append(precision)
		test_recalls.append(recall)
		test_f_measures.append(f_measure)
		test_accuracies.append(accuracy)	
		test_auroc_scores.append(auroc_score)

	training_mean_precision = np.mean(training_precisions)
	training_mean_recall = np.mean(training_recalls)
	training_mean_f_measure = np.mean(training_f_measures)
	training_mean_accuracy = np.mean(training_accuracies)	
	training_mean_auroc_score = np.mean(training_auroc_scores)

	training_std_precision = np.std(training_precisions)
	training_std_recall = np.std(training_recalls)
	training_std_f_measure = np.std(training_f_measures)
	training_std_accuracy = np.std(training_accuracies)	
	training_std_auroc_score = np.std(training_auroc_scores)	

	test_mean_precision = np.mean(test_precisions)
	test_mean_recall = np.mean(test_recalls)
	test_mean_f_measure = np.mean(test_f_measures)
	test_mean_accuracy = np.mean(test_accuracies)
	test_mean_auroc_score = np.mean(test_auroc_scores)	

	test_std_precision = np.std(test_precisions)
	test_std_recall = np.std(test_recalls)
	test_std_f_measure = np.std(test_f_measures)
	test_std_accuracy = np.std(test_accuracies)	
	test_std_auroc_score = np.std(test_auroc_scores)

	print("Training conf int precision: %0.3f (+/- %0.3f)" % (training_mean_precision, training_std_precision * 2))
	print("Training conf int recall: %0.3f (+/- %0.3f)" % (training_mean_recall, training_std_recall * 2))
	print("Training conf int f_measure: %0.3f (+/- %0.3f)" % (training_mean_f_measure, training_std_f_measure * 2))
	print("Training conf int accuracy: %0.3f (+/- %0.3f)" % (training_mean_accuracy, training_std_accuracy * 2))
	print("Training conf int AUROC: %0.3f (+/- %0.3f)" % (training_mean_auroc_score, training_std_auroc_score * 2))

	print("Test conf int precision: %0.3f (+/- %0.3f)" % (test_mean_precision, test_std_precision * 2))
	print("Test conf int recall: %0.3f (+/- %0.3f)" % (test_mean_recall, test_std_recall * 2))
	print("Test conf int f_measure: %0.3f (+/- %0.3f)" % (test_mean_f_measure, test_std_f_measure * 2))
	print("Test conf int accuracy: %0.3f (+/- %0.3f)" % (test_mean_accuracy, test_std_accuracy * 2))
	print("Test conf int AUROC: %0.3f (+/- %0.3f)" % (test_mean_auroc_score, test_std_auroc_score * 2))

	return predictor
	# return test_mean_precision, test_mean_recall, test_mean_f_measure, test_mean_accuracy




def evaluate_performance_predictor(test_labels, predicted_labels, predicted_probabilities, label_string):
	# test precision
	precision = precision_score(test_labels, predicted_labels, average=None)	
	# test recall	
	recall = recall_score(test_labels, predicted_labels, average=None)	
	# test F1 score	
	f_measure = f1_score(test_labels, predicted_labels, average=None)	
	# ROC
	fpr = dict()
	tpr = dict()
	roc_auc = dict()
	fpr, tpr, thresholds = roc_curve(test_labels, predicted_probabilities, pos_label=label_string)	
	roc_auc = auc(fpr, tpr)
	auroc_score = roc_auc_score(test_labels, predicted_probabilities)

	return precision[0], recall[0], f_measure[0], auroc_score
	# return precision[0], recall[0], f_measure[0]



	
def train_predictor(training_features, training_labels):	
	predictor = SVC(kernel='linear', C=1, gamma=1, probability=True)	
	predictor = predictor.fit(training_features, training_labels)

	return predictor	




def build_training_test_sets(features, labels):	
	vector = list(zip(features, labels))	
	random.shuffle(vector)
	features, labels = zip(*vector)

	# split dataset and labels into randomized training and test sets, with training set being a given percentage of the set
	training_features, test_features, training_labels, test_labels = train_test_split(features, labels, train_size=0.8, test_size=0.2)

	return training_features, test_features, training_labels, test_labels




def build_features_labels(corpus, complement_corpus, label_string, complement_label_string):	
	whole_corpus = corpus + complement_corpus	

	# the model was defined with default 100 features
	num_features = 100

	# extract trained vector representation of the training sets	
	feature_vector = get_average_feature_vecs(whole_corpus, model, num_features)	
	# with open(average_vector_path, 'a') as f:
	# 	for v in feature_vector:
	# 		f.write(str(v))

	# define labels
	labels = np.full(len(corpus), label_string)
	complement_labels = np.full(len(complement_corpus), complement_label_string)
	wholeset_labels = np.concatenate((labels, complement_labels))

	lb = LabelEncoder()
	wholeset_labels = lb.fit_transform(wholeset_labels)

	return feature_vector, wholeset_labels	




def make_feature_vec(words, model, num_features):    
	# pre-initialize an empty numpy array (for speed)
	featureVec = np.zeros((num_features,),dtype="float32")    
	nwords = 0
	 
	# Index2word is a list that contains the names of the words in the model's vocabulary. Convert it to a set, for speed 
	index2word_set = set(model.wv.index2word)
		
	for word in words:
		if word in index2word_set: 
			nwords = nwords + 1
			featureVec = np.add(featureVec,model[word])
		
	if nwords != 0:
		featureVec = np.divide(featureVec,nwords)
	else:
		featureVec = np.zeros((1,),dtype="float32")
	return featureVec




def get_average_feature_vecs(tweets, model, num_features):	
	counter = 0
	# preallocate a 2D numpy array for speed
	tweetFeatureVecs = np.zeros((len(tweets),num_features),dtype="float32")
		
	for tweet in tweets:	   
	   tweetFeatureVecs[counter] = make_feature_vec(tweet, model, num_features)	   	  
	   counter = counter + 1
	   print(counter)

	return tweetFeatureVecs




def build_corpus_with_keywords(select_tweets_query, keywords, words_path, preprocessed_tweets_path):
	# create a list of tweets
	corpus_raw = build_raw_corpus(select_tweets_query)

	# write to file the list of tweets	
	with open(words_path, 'w') as f:
		f.write(str(corpus_raw))	

	corpus = []
	# pre-process the corpus
	for tweet in corpus_raw:
		corpus.append(apply_preprocessing_to_corpus_with_keywords(tweet, keywords))
	
	with open(preprocessed_tweets_path, 'w') as f:
		f.write(str(corpus))	
	
	return corpus



def apply_preprocessing_to_corpus_with_keywords(tweet, keywords):
	tokenizer = TweetTokenizer()
	# tokenize tweet text
	tweet_tokenized = tokenizer.tokenize(' '.join(tweet))
	# remove the URLs 
	tweet_url = [w for w in tweet_tokenized if 'http' not in w.lower()]
	# remove punctuation
	tweet_punct = [remove_extra_chars_from_word(w) for w in tweet_url]
	# remove the polarized keywords	
	tweet_keywords = [w for w in tweet_punct if w.lower() not in keywords]
	# remove stop-words	
	stop_words = set(stopwords.words('italian'))
	tweet = [w for w in tweet_keywords if w.lower() not in stop_words]
	# remove any '' word as result of preceding processing
	tweet = [w for w in tweet if w != '']

	return tweet




def build_corpus(select_tweets_query, words_path, preprocessed_tweets_path):
	# create a list of tweets
	corpus_raw = build_raw_corpus(select_tweets_query)

	# write to file the list of tweets	
	with open(words_path, 'w') as f:
		f.write(str(corpus_raw))	

	corpus = []
	# pre-process the corpus
	for tweet in corpus_raw:
		corpus.append(apply_preprocessing_to_corpus(tweet))
	
	with open(preprocessed_tweets_path, 'w') as f:
		f.write(str(corpus))	
	
	return corpus



def apply_preprocessing_to_corpus(tweet):
	tokenizer = TweetTokenizer()
	# tokenize tweet text
	tweet_tokenized = tokenizer.tokenize(' '.join(tweet))
	# remove the URLs 
	tweet_url = [w for w in tweet_tokenized if 'http' not in w.lower()]
	# remove punctuation
	tweet_punct = [remove_extra_chars_from_word(w) for w in tweet_url]
	# remove stop-words	
	stop_words = set(stopwords.words('italian'))
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




def build_raw_corpus(select_tweets_query):
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




def prepare_keywords():
	# print('Preparing keywords.....')

	with open(negative_keywords_path, encoding='utf-8') as f:
		for line in f:
			# prepend an hashtag
			a = '#' + line.strip()		
			negative_keywords.append(a)
	with open(positive_keywords_path, encoding='utf-8') as f:
		for line in f:
			# prepend an hashtag
			a = '#' + line.strip()		
			positive_keywords.append(a)	






###############################################################################################################
# Application: apply the predictors on the unclassified tweets
###############################################################################################################	

def update_users_political_alignment():
	users = set()

	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_users_polarized_tweets_query)
		rows = cur.fetchall()			
		
		for row in rows:			
			userid = row[0]			
			users.add(userid)
		
		cur.close()
		conn.close()		
	except psycopg2.Error as e:
		print(e.pgerror)
		print("Select user error: " + str(e))	

	count = 0
	for user in users:
		try:
			conn = psycopg2.connect(conn_string)
			cur = conn.cursor()
			cur.execute("SELECT polarity FROM " + tweets_table + " WHERE userid = %s", (user, ))
			rows = cur.fetchall()			
			
			neg_count = 0
			pos_count = 0
			neu_count = 0

			for row in rows:			
				polarity = row[0]			
				if polarity == 'Negative':
					neg_count += 1
				elif polarity == 'Positive':
					pos_count += 1
				elif polarity == 'Neutral':
					neu_count += 1

			try:
				conn2 = psycopg2.connect(conn_string)
				cur2 = conn2.cursor()
				
				if neg_count > pos_count and neg_count > neu_count:
					count += 1
					print(str(count) + ': user polarity assignment')								
					cur2.execute("UPDATE " + users_table + " SET politicalview = %s WHERE userid = %s", ('Negative', user, ))					
				elif pos_count > neg_count and pos_count > neu_count:
					count += 1
					print(str(count) + ': user polarity assignment')
					cur2.execute("UPDATE " + users_table + " SET politicalview = %s WHERE userid = %s", ('Positive', user, ))					
				else:
					count += 1
					print(str(count) + ': user polarity assignment')
					cur2.execute("UPDATE " + users_table + " SET politicalview = %s WHERE userid = %s", ('Neutral', user, ))					
				
				conn2.commit()	
				cur2.close()
				conn2.close()			
			except Exception as e:
				print("Update user alignment error: " + str(e))
				pass
			
			cur.close()
			conn.close()		
		except psycopg2.Error as e:
			print(e.pgerror)
			print("Select user's tweets error: " + str(e))






def apply_polarity_predictor_to_relevant_data(negative_predictor, positive_predictor, relevant_df):
	print()
	print()	

	# Test phase predictor
	num_features = 100	
	features = get_average_feature_vecs(relevant_df['tweet_contents'], model, num_features)
	relevant_df['negative_labels'] = negative_predictor.predict(features)
	relevant_df['positive_labels'] = positive_predictor.predict(features)
	# print(relevant_df.head()) 
	# print('Writing to file predicted labels for the test data....')
	# with open(relevant_labels_path, 'w') as f:
		# f.write(str(predicted_labels))
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()

		count = 0
		for index, row in relevant_df.iterrows(): 
			# Negative: 0 - Non-negative: 1
			# Positive: 1 - Non-positive: 0 
			if row['negative_labels'] == 0 or row['positive_labels'] == 1:
				if row['negative_labels'] == 0 and row['positive_labels'] == 0:
					count += 1	
					print(str(count) + ': relevant tweet polarity assignment')			
					cur.execute("UPDATE " + tweets_table + " SET polarity = %s WHERE tweetid = %s", ('Negative', row['tweetids'], ))					
					conn.commit()
				elif row['negative_labels'] == 1 and row['positive_labels'] == 1:
					count += 1	
					print(str(count) + ': relevant tweet polarity assignment')
					cur.execute("UPDATE " + tweets_table + " SET polarity = %s WHERE tweetid = %s", ('Positive', row['tweetids'], ))					
					conn.commit()
			else:
				count += 1	
				print(str(count) + ': relevant tweet polarity assignment')
				cur.execute("UPDATE " + tweets_table + " SET polarity = %s WHERE tweetid = %s", ('Neutral', row['tweetids'], ))					
				conn.commit()
		cur.close()
		conn.close()			
	except Exception as e:
		print("Update Userrole Error: " + str(e))
		pass





def apply_predictor_to_unclassified_data(predictor, unclassified_df):
	print()
	print()	

	# Test phase predictor
	num_features = 100	
	features = get_average_feature_vecs(unclassified_df['tweet_contents'], model, num_features)
	unclassified_df['labels'] = predictor.predict(features)
	print(unclassified_df.head()) 
	# print('Writing to file predicted labels for the test data....')
	# with open(unclassified_labels_path, 'w') as f:
		# f.write(str(predicted_labels))

	count = 0
	for index, row in unclassified_df.iterrows(): 
		try: 
			conn = psycopg2.connect(conn_string)
			cur = conn.cursor() 
			if row['labels'] == 1:
				count += 1	
				print(str(count) + ': tweet polarity assignment')
				cur.execute("UPDATE " + tweets_table + " SET polarity = %s WHERE tweetid = %s", ('Relevant', row['tweetids'], ))
				conn.commit()
			else:
				count += 1	
				print(str(count) + ': tweet polarity assignment')
				cur.execute("UPDATE " + tweets_table + " SET polarity = %s WHERE tweetid = %s", ('Non Relevant', row['tweetids'], ))
				conn.commit()
			cur.close()
			conn.close()	
		except Exception as e:
			print("Update Userrole Error: " + str(e))
			pass




def build_unclassified_data(select_unclassified_tweets_query, select_unclassified_tweetids_query):
	# create a list of tweets
	corpus_raw = build_raw_corpus(select_unclassified_tweets_query)	

	corpus = []
	# pre-process the corpus
	for tweet in corpus_raw:
		corpus.append(apply_preprocessing_to_corpus(tweet))
	
	# num_features = 100	
	# features = get_average_feature_vecs(corpus, model, num_features)	
			
	tweetids = retrieve_tweetids(select_unclassified_tweetids_query)

	unclassified_df = pd.DataFrame(data=tweetids, columns=['tweetids'])
	unclassified_df['tweet_contents'] = corpus
	# for i in range(len(features)):
	# 	unclassified_df['tweet_contents'] = pd.Series([features[i]], index=unclassified_df.index)
	
	return unclassified_df




def retrieve_tweetids(select_tweets_query):
	tweetids = []
	
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_tweets_query)
		tweetids = cur.fetchall()				
		
		cur.close()
		conn.close()		
	except psycopg2.Error as e:
		print(e.pgerror)
		print("Connection failed")

	return tweetids

###############################################################################################################






###############################################################################################################
# Test: one three-class predictor for positive, negative, neutral
###############################################################################################################

def build_train_test_evaluate_multinomial_predictor(features, labels, label_string):
	training_precisions = []
	training_recalls = []
	training_f_measures = []
	training_accuracies = []
	# training_auroc_scores = []
	test_precisions = []
	test_recalls = []
	test_f_measures = []
	test_accuracies = []	
	# test_auroc_scores = []	
	# test_features_iterations = []
	# test_labels_iterations = []

	for i in range(10):
		training_features, test_features, training_labels, test_labels = build_training_test_sets(features, labels)
		# test_features_iterations.append(test_features)		
		# test_labels_iterations.append(test_labels)
		predictor = train_predictor(training_features, training_labels)

		predicted_labels = predictor.predict(training_features)
		predicted_probabilities = predictor.predict_proba(training_features)
		precision, recall, f_measure = evaluate_performance_predictor(training_labels, predicted_labels, predicted_probabilities[:,0], label_string)	
		accuracy = predictor.score(training_features, training_labels)

		training_precisions.append(precision)
		training_recalls.append(recall)
		training_f_measures.append(f_measure)
		training_accuracies.append(accuracy)
		# training_auroc_scores.append(auroc_score)
		
		predicted_labels = predictor.predict(test_features)
		predicted_probabilities = predictor.predict_proba(test_features)
		precision, recall, f_measure = evaluate_performance_predictor(test_labels, predicted_labels, predicted_probabilities[:,0], label_string)	
		accuracy = predictor.score(test_features, test_labels)

		test_precisions.append(precision)
		test_recalls.append(recall)
		test_f_measures.append(f_measure)
		test_accuracies.append(accuracy)	
		# test_auroc_scores.append(auroc_score)

	training_mean_precision = np.mean(training_precisions)
	training_mean_recall = np.mean(training_recalls)
	training_mean_f_measure = np.mean(training_f_measures)
	training_mean_accuracy = np.mean(training_accuracies)	
	# training_mean_auroc_score = np.mean(training_auroc_scores)

	training_std_precision = np.std(training_precisions)
	training_std_recall = np.std(training_recalls)
	training_std_f_measure = np.std(training_f_measures)
	training_std_accuracy = np.std(training_accuracies)	
	# training_std_auroc_score = np.std(training_auroc_scores)	

	test_mean_precision = np.mean(test_precisions)
	test_mean_recall = np.mean(test_recalls)
	test_mean_f_measure = np.mean(test_f_measures)
	test_mean_accuracy = np.mean(test_accuracies)
	# test_mean_auroc_score = np.mean(test_auroc_scores)	

	test_std_precision = np.std(test_precisions)
	test_std_recall = np.std(test_recalls)
	test_std_f_measure = np.std(test_f_measures)
	test_std_accuracy = np.std(test_accuracies)	
	# test_std_auroc_score = np.std(test_auroc_scores)

	print("Training conf int precision: %0.3f (+/- %0.3f)" % (training_mean_precision, training_std_precision * 2))
	print("Training conf int recall: %0.3f (+/- %0.3f)" % (training_mean_recall, training_std_recall * 2))
	print("Training conf int f_measure: %0.3f (+/- %0.3f)" % (training_mean_f_measure, training_std_f_measure * 2))
	print("Training conf int accuracy: %0.3f (+/- %0.3f)" % (training_mean_accuracy, training_std_accuracy * 2))
	# print("Training conf int AUROC: %0.3f (+/- %0.3f)" % (training_mean_auroc_score, training_std_auroc_score * 2))

	print("Test conf int precision: %0.3f (+/- %0.3f)" % (test_mean_precision, test_std_precision * 2))
	print("Test conf int recall: %0.3f (+/- %0.3f)" % (test_mean_recall, test_std_recall * 2))
	print("Test conf int f_measure: %0.3f (+/- %0.3f)" % (test_mean_f_measure, test_std_f_measure * 2))
	print("Test conf int accuracy: %0.3f (+/- %0.3f)" % (test_mean_accuracy, test_std_accuracy * 2))
	# print("Test conf int AUROC: %0.3f (+/- %0.3f)" % (test_mean_auroc_score, test_std_auroc_score * 2))




def build_features_multinomial_labels(corpus, negative_label_string, positive_label_string, neutral_label_string):	
	num_features = 100
	labels_length = 400

	features = get_average_feature_vecs(corpus, model, num_features)	

	# define labels
	negative_labels = np.full(labels_length, negative_label_string)
	positive_labels = np.full(labels_length, positive_label_string)
	neutral_labels = np.full(labels_length, neutral_label_string)
	wholeset_labels = np.concatenate((negative_labels, positive_labels, neutral_labels))
	# print(wholeset_labels[:5])

	lb = LabelEncoder()
	wholeset_labels = lb.fit_transform(wholeset_labels)

	# print(wholeset_labels[:5])

	print(lb.inverse_transform([0, 1, 2]))

	return features, wholeset_labels

###############################################################################################################











experiment_driver()