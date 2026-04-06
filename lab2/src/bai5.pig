raw_data = LOAD 'hotel-review.csv'
USING PigStorage(';')
AS (
    id:chararray,
    review:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

reviews_lower = FOREACH raw_data GENERATE
    id,
    LOWER(review) AS review_lower,
    category,
    aspect,
    sentiment;

tokenized = FOREACH reviews_lower GENERATE
    id,
    FLATTEN(TOKENIZE(review_lower)) AS word,
    category,
    aspect,
    sentiment;

stopwords = LOAD 'stopwords.txt'
USING PigStorage('\t')
AS (stopword:chararray);

filtered_words = FILTER tokenized BY word IS NOT NULL AND SIZE(word) > 0;

joined_stop = JOIN filtered_words BY word LEFT OUTER, stopwords BY stopword;

clean_words = FILTER joined_stop BY stopwords::stopword IS NULL;

clean_result = FOREACH clean_words GENERATE
    filtered_words::id AS id,
    filtered_words::word AS word,
    filtered_words::category AS category,
    filtered_words::aspect AS aspect,
    filtered_words::sentiment AS sentiment;

group_cat_word = GROUP clean_result BY (category, word);

count_cat_word = FOREACH group_cat_word GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(clean_result) AS freq;

group_by_cat = GROUP count_cat_word BY category;

top5_related_each_cat = FOREACH group_by_cat {
    sorted_words = ORDER count_cat_word BY freq DESC;
    top5_words = LIMIT sorted_words 5;
    GENERATE FLATTEN(top5_words);
};

STORE top5_related_each_cat INTO 'output/bai5_output/top5_related_words_by_category' USING PigStorage(';');

