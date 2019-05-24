-- Hybrid recommendation model on MovieLens data

WITH userFeatures AS (
  SELECT 
     u.*,
     (SELECT ARRAY_AGG(weight) FROM UNNEST(factor_weights)) AS user_factors
  FROM
     ch09eu.movielens_users u
  JOIN
     ML.WEIGHTS(MODEL ch09eu.movie_recommender_16) w
  ON
     processed_input = 'userId' AND feature = CAST(u.userId AS STRING)
),

productFeatures AS (
  SELECT 
     p.* EXCEPT(genres)
     , g
     , (SELECT ARRAY_AGG(weight) FROM UNNEST(factor_weights)) AS product_factors
  FROM
     ch09eu.movielens_movies p, UNNEST(genres) g
  JOIN
     ML.WEIGHTS(MODEL ch09eu.movie_recommender_16) w
  ON
     processed_input = 'movieId' AND feature = CAST(p.movieId AS STRING)
)

SELECT p.* EXCEPT(movieId), u.* EXCEPT(userId), rating 
FROM productFeatures p, userFeatures u
JOIN
   ch09eu.movielens_ratings r
ON
   r.movieId = p.movieId AND r.userId = u.userId
LIMIT 5


