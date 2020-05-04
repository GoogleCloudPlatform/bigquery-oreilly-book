CREATE OR REPLACE MODEL advdata.ga360_recommendations_model
OPTIONS(model_type='matrix_factorization',
        user_col='visitorId', item_col='contentId',
        rating_col='normalized_session_duration',
        l2_reg=10)
AS
SELECT * from advdata.ga360_recommendations_data
