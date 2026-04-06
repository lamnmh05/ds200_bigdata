raw_data = LOAD 'hotel-review.csv'
USING PigStorage(';')
AS (
    id:chararray,
    review:chararray,
    category:chararray,
    aspect:chararray,
    sentiment:chararray
);

review_aspect_sentiment = FOREACH raw_data GENERATE id, aspect, sentiment;
distinct_pairs = DISTINCT review_aspect_sentiment;

negative_only = FILTER distinct_pairs BY sentiment == 'negative';
group_neg = GROUP negative_only BY aspect;

count_neg = FOREACH group_neg GENERATE
    group AS aspect,
    COUNT(negative_only) AS negative_count;

count_neg_sorted = ORDER count_neg BY negative_count DESC;
top_negative = LIMIT count_neg_sorted 1;

positive_only = FILTER distinct_pairs BY sentiment == 'positive';
group_pos = GROUP positive_only BY aspect;

count_pos = FOREACH group_pos GENERATE
    group AS aspect,
    COUNT(positive_only) AS positive_count;

count_pos_sorted = ORDER count_pos BY positive_count DESC;
top_positive = LIMIT count_pos_sorted 1;

STORE top_negative INTO 'output/bai3_output/top_negative_aspect' USING PigStorage(';');
STORE top_positive INTO 'output/bai3_output/top_positive_aspect' USING PigStorage(';');
